package campaign

import (
	"context"
	"github.com/viant/xdatly/handler"
)

type Handler struct {
}

func (h *Handler) Exec(ctx context.Context, session handler.Session) (interface{}, error) {
	state := &State{}
	if err := session.Stater().Into(ctx, state); err != nil {
		return nil, err
	}

	curCampaigns := state.CurCampaign
	/*
		#set($FlightsById = $CurFlights.IndexBy("Id"))

	*/
	campaignById := CampaignsSlice(curCampaigns).IndexBy("Id")

	campaignById.Has()
	return "123", nil
}
