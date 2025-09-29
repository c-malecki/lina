package action

import (
	"context"

	"github.com/c-malecki/lina/internal/action/network"
	"github.com/c-malecki/lina/internal/dbw"
)

type APP_ACTION string

const (
	UPDATE_CONNECTIONS APP_ACTION = "1"
	SEARCH             APP_ACTION = "2"
	UPDATE_APIFY       APP_ACTION = "3"
	QUIT               APP_ACTION = "4"
)

func (aa APP_ACTION) String() string {
	switch aa {
	case UPDATE_CONNECTIONS:
		return "Update Connections"
	case SEARCH:
		return "Search"
	case UPDATE_APIFY:
		return "Update Apify Token"
	case QUIT:
		return "Quit"
	default:
		return "Invalid Option"
	}
}

func ActionUpdateConnections(ctx context.Context, dbw *dbw.DBW, token string, networkId int64) error {
	urls, err := network.ParseLinkedinCsv()
	if err != nil {
		return err
	}

	newUrls, err := network.CompareConnections(ctx, dbw, networkId, urls)
	if err != nil {
		return err
	}

	if len(newUrls) == 0 {
		return nil
	}

	return nil
}
