package export

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/TicketsBot-cloud/database"
	"github.com/minio/minio-go/v7"
)

func ObjectKey(t database.GdprExportType, id int64) string {
	return fmt.Sprintf("%s/%d.zip", t, id)
}

func Upload(ctx context.Context, client *minio.Client, bucket, key string, write func(context.Context, *zip.Writer) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	pr, pw := io.Pipe()
	writeErr := make(chan error, 1)

	go func() {
		zw := zip.NewWriter(pw)
		err := write(ctx, zw)
		if err == nil {
			err = zw.Close()
		}

		pw.CloseWithError(err)
		writeErr <- err
	}()

	// Without a part size, an upload of unknown length buffers parts of about 537 MiB.
	_, err := client.PutObject(ctx, bucket, key, pr, -1, minio.PutObjectOptions{ContentType: "application/zip", PartSize: 5 << 20})
	if err != nil {
		pr.CloseWithError(err)
		cancel()
	}

	if writerErr := <-writeErr; err == nil {
		err = writerErr
	}

	if err != nil {
		// minio aborts the upload with the request context, which may already be done.
		abortCtx, abortCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer abortCancel()

		if abortErr := client.RemoveIncompleteUpload(abortCtx, bucket, key); abortErr != nil {
			err = errors.Join(err, abortErr)
		}
	}

	return err
}
