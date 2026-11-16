package archiver

import (
	"net/http"
	"time"

	"github.com/TicketsBot-cloud/archiverclient"
	"go.uber.org/zap"
)

var (
	Client *archiverclient.ArchiverClient
	Proxy  *archiverclient.ProxyRetriever
)

func Initialize(logger *zap.Logger, url, aesKey string) {
	// The default 3s covers reading the body too, which large transcripts in exports and re-uploads exceed.
	Proxy = archiverclient.NewProxyRetrieverWithClient(&http.Client{Timeout: 30 * time.Second}, url)
	Client = archiverclient.NewArchiverClient(
		Proxy,
		[]byte(aesKey),
	)

	logger.Info("Archiver client initialized")
}
