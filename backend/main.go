package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/kelseyhightower/envconfig"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"google.golang.org/api/iterator"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	instancepb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"
)

type Config struct {
	GCloudProject     string `required:"true" envconfig:"GCLOUD_PROJECT"`
	SpannerInstanceID string `required:"true" split_words:"true"`
	SpannerDatabaseID string `required:"true" split_words:"true"`
}

func main() {
	var cfg Config
	err := envconfig.Process("myapp", &cfg)
	if err != nil {
		log.Fatal(err.Error())
	}

	format := `
	GCloud Project      : %v
	Spanner Instance ID : %s
	Spanner Database ID : %s
	Use Spanner Emu     : %t (%s)
	`

	spannerEmuHost, isUseEmu := os.LookupEnv("SPANNER_EMULATOR_HOST")
	_, err = fmt.Printf(format, cfg.GCloudProject, cfg.SpannerInstanceID, cfg.SpannerDatabaseID, isUseEmu, spannerEmuHost)
	if err != nil {
		log.Fatal(err.Error())
	}

	ctx := context.Background()

	if isUseEmu {
		log.Print("Creating Spanner instance ...")
		if err := createInstance(ctx, cfg.GCloudProject, cfg.SpannerInstanceID); err != nil {
			log.Fatal(err)
		}

		log.Print("Creating Spanner database ...")
		if err := createDB(ctx, cfg.GCloudProject, cfg.SpannerInstanceID, cfg.SpannerDatabaseID); err != nil {
			log.Fatal(err)
		}
	}

	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", cfg.GCloudProject, cfg.SpannerInstanceID, cfg.SpannerDatabaseID)

	log.Print("Inserting data into tables: Singers, Albums ...")
	if err := insertOrUpdate(ctx, dbPath); err != nil {
		log.Fatal(err)
	}

	log.Print("Adding a MarketingBudget column to table Albums ...")
	if err := addMarketingBudgetColumn(ctx, dbPath); err != nil {
		log.Fatal(err)
	}

	log.Print("Updating MarketingBudgets ...")
	if err := updateMarketingBudgets(ctx, dbPath); err != nil {
		log.Fatal(err)
	}

	log.Print("Transferring MarketingBudgets ...")
	if err := transferMarketingBudgets(ctx, dbPath); err != nil {
		log.Fatal(err)
	}

	log.Print("HTTP server listening on port 8000")

	r := mux.NewRouter()

	r.HandleFunc("/albums", func(w http.ResponseWriter, r *http.Request) {
		albums, err := getAlbums(r.Context(), dbPath, 3)
		if err != nil {
			log.Printf("Error: %s", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		enc.Encode(albums)
	})

	log.Fatal(http.ListenAndServe(":8000", handlers.LoggingHandler(os.Stdout, r)))
}

type Album struct {
	SingerID        int64             `json:"singer_id"`
	AlbumID         int64             `json:"album_id"`
	MarketingBudget spanner.NullInt64 `json:"marketing_budget"`
	LastUpdateTime  spanner.NullTime  `json:"last_update_time"`
}

func getAlbums(ctx context.Context, dbPath string, max int) (albums []*Album, err error) {
	var client *spanner.Client

	client, err = spanner.NewClient(ctx, dbPath)
	if err != nil {
		return
	}
	defer client.Close()

	stmt := spanner.Statement{
		SQL: `SELECT SingerId, AlbumId, MarketingBudget, LastUpdateTime
              FROM Albums
			  ORDER BY LastUpdateTime DESC
			  LIMIT @max
			  `,
		Params: map[string]interface{}{
			"max": max,
		},
	}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	for {
		var row *spanner.Row
		row, err = iter.Next()
		if err == iterator.Done {
			err = nil
			return
		}
		if err != nil {
			return
		}

		a := new(Album)

		if err = row.ColumnByName("SingerId", &a.SingerID); err != nil {
			return
		}
		if err = row.ColumnByName("AlbumId", &a.AlbumID); err != nil {
			return
		}
		if err = row.ColumnByName("MarketingBudget", &a.MarketingBudget); err != nil {
			return
		}
		if err = row.ColumnByName("LastUpdateTime", &a.LastUpdateTime); err != nil {
			return
		}

		albums = append(albums, a)
	}
}

func transferMarketingBudgets(ctx context.Context, dbPath string) error {
	client, err := spanner.NewClient(ctx, dbPath)
	if err != nil {
		return err
	}
	defer client.Close()

	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// getBudget returns the budget for a record with a given albumId and singerId.
		getBudget := func(albumID, singerID int64) (int64, error) {
			key := spanner.Key{albumID, singerID}
			row, err := txn.ReadRow(ctx, "Albums", key, []string{"MarketingBudget"})
			if err != nil {
				return 0, err
			}
			var budget int64
			if err := row.Column(0, &budget); err != nil {
				return 0, err
			}
			return budget, nil
		}
		// updateBudget updates the budget for a record with a given albumId and singerId.
		updateBudget := func(singerID, albumID, albumBudget int64) error {
			stmt := spanner.Statement{
				SQL: `UPDATE Albums
                      SET MarketingBudget = @AlbumBudget
                      WHERE SingerId = @SingerId and AlbumId = @AlbumId`,
				Params: map[string]interface{}{
					"SingerId":    singerID,
					"AlbumId":     albumID,
					"AlbumBudget": albumBudget,
				},
			}
			_, err := txn.Update(ctx, stmt)
			return err
		}

		// Transfer the marketing budget from one album to another. By keeping the actions
		// in a single transaction, it ensures the movement is atomic.
		const transferAmt = 200000

		album2Budget, err := getBudget(2, 2)
		if err != nil {
			return err
		}

		// The transaction will only be committed if this condition still holds at the time
		// of commit. Otherwise it will be aborted and the callable will be rerun by the
		// client library.
		if album2Budget >= transferAmt {
			album1Budget, err := getBudget(1, 1)
			if err != nil {
				return err
			}

			if err = updateBudget(1, 1, album1Budget+transferAmt); err != nil {
				return err
			}

			if err = updateBudget(2, 2, album2Budget-transferAmt); err != nil {
				return err
			}

			stmt := spanner.Statement{
				SQL: `UPDATE Albums
                      SET LastUpdateTime = PENDING_COMMIT_TIMESTAMP()
                      WHERE SingerId IN UNNEST(@SingerIds) AND AlbumId IN UNNEST(@AlbumIds)`,
				Params: map[string]interface{}{
					"SingerIds": []int64{1, 2},
					"AlbumIds":  []int64{1, 2},
				},
			}
			if _, err = txn.Update(ctx, stmt); err != nil {
				return err
			}

			log.Printf("Moved %d from Album2's MarketingBudget to Album1's", transferAmt)
		}
		return nil
	})
	return err
}

func updateMarketingBudgets(ctx context.Context, dbPath string) error {
	client, err := spanner.NewClient(ctx, dbPath)
	if err != nil {
		return err
	}
	defer client.Close()

	cols := []string{"SingerId", "AlbumId", "MarketingBudget"}
	_, err = client.Apply(ctx, []*spanner.Mutation{
		spanner.Update("Albums", cols, []interface{}{1, 1, 100000}),
		spanner.Update("Albums", cols, []interface{}{2, 2, 500000}),
	})
	return err
}

func addMarketingBudgetColumn(ctx context.Context, dbPath string) error {
	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	defer adminClient.Close()

	op, err := adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database: dbPath,
		Statements: []string{
			"ALTER TABLE Albums ADD COLUMN MarketingBudget INT64",
		},
	})
	if err != nil {
		return err
	}
	if err := op.Wait(ctx); err != nil {
		return err
	}

	log.Print("Added MarketingBudget column to table Albums")

	return nil
}

func insertOrUpdate(ctx context.Context, dbPath string) error {
	client, err := spanner.NewClient(ctx, dbPath)
	if err != nil {
		return err
	}
	defer client.Close()

	singerColumns := []string{"SingerId", "FirstName", "LastName"}
	albumColumns := []string{"SingerId", "AlbumId", "AlbumTitle", "LastUpdateTime"}

	m := []*spanner.Mutation{
		spanner.InsertOrUpdate("Singers", singerColumns, []interface{}{1, "Marc", "Richards"}),
		spanner.InsertOrUpdate("Singers", singerColumns, []interface{}{2, "Catalina", "Smith"}),
		spanner.InsertOrUpdate("Singers", singerColumns, []interface{}{3, "Alice", "Trentor"}),
		spanner.InsertOrUpdate("Singers", singerColumns, []interface{}{4, "Lea", "Martin"}),
		spanner.InsertOrUpdate("Singers", singerColumns, []interface{}{5, "David", "Lomond"}),
		spanner.InsertOrUpdate("Albums", albumColumns, []interface{}{1, 1, "Total Junk", spanner.CommitTimestamp}),
		spanner.InsertOrUpdate("Albums", albumColumns, []interface{}{1, 2, "Go, Go, Go", spanner.CommitTimestamp}),
		spanner.InsertOrUpdate("Albums", albumColumns, []interface{}{2, 1, "Green", spanner.CommitTimestamp}),
		spanner.InsertOrUpdate("Albums", albumColumns, []interface{}{2, 2, "Forever Hold Your Peace", spanner.CommitTimestamp}),
		spanner.InsertOrUpdate("Albums", albumColumns, []interface{}{2, 3, "Terrified", spanner.CommitTimestamp}),
	}

	_, err = client.Apply(ctx, m)

	return err
}

func createInstance(ctx context.Context, projectID, instanceID string) error {
	instanceAdmin, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		return err
	}
	defer instanceAdmin.Close()

	op, err := instanceAdmin.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", projectID),
		InstanceId: instanceID,
		Instance: &instancepb.Instance{
			Config:      fmt.Sprintf("projects/%s/instanceConfigs/%s", projectID, "regional-us-central1"),
			DisplayName: instanceID,
			NodeCount:   1,
			Labels:      map[string]string{"cloud_spanner_samples": "true"},
		},
	})
	if err != nil {
		return fmt.Errorf("could not create instance %s: %v", fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID), err)
	}
	// Wait for the instance creation to finish.
	i, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("waiting for instance creation to finish failed: %v", err)
	}

	// The instance may not be ready to serve yet.
	if i.State != instancepb.Instance_READY {
		fmt.Printf("instance state is not READY yet. Got state %v\n", i.State)
	}

	log.Printf("Created instance [%s]", instanceID)

	return nil

}

func createDB(ctx context.Context, projectID, instanceID, databaseID string) error {
	c, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	defer c.Close()

	op, err := c.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID),
		CreateStatement: "CREATE DATABASE `" + databaseID + "`",
		ExtraStatements: []string{
			`CREATE TABLE Singers (
                SingerId   		INT64 NOT NULL,
                FirstName  		STRING(1024),
                LastName   		STRING(1024),
                SingerInfo 		BYTES(MAX)
        	) PRIMARY KEY (SingerId)`,
			`CREATE TABLE Albums (
				SingerId        INT64 NOT NULL,
				AlbumId         INT64 NOT NULL,
				AlbumTitle      STRING(MAX),
				LastUpdateTime  TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
			) PRIMARY KEY (SingerId, AlbumId),
			INTERLEAVE IN PARENT Singers ON DELETE CASCADE`,
		},
	})
	if err != nil {
		return err
	}
	if _, err := op.Wait(ctx); err != nil {
		return err
	}

	log.Printf("Created database [%s / %s]\n", instanceID, databaseID)

	return nil

}
