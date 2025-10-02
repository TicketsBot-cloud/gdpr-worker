package archiver

import (
	"github.com/TicketsBot-cloud/archiverclient"
	"go.uber.org/zap"
)

var (
	Client *archiverclient.ArchiverClient
	Proxy  *archiverclient.ProxyRetriever
)

func Initialize(logger *zap.Logger, url, aesKey string) {
	Proxy = archiverclient.NewProxyRetriever(url)
	Client = archiverclient.NewArchiverClient(
		Proxy,
		[]byte(aesKey),
	)

	logger.Info("Archiver client initialized", zap.String("url", url))
}
