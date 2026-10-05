package console

import (
	"context"
	"log/slog"
	"math/rand"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	sfv1 "github.com/sohamb17/streamforge/gen/streamforge/v1"
)

// busyZones are the zones with the most pickups in the TLC month.
var busyZones = []int{161, 237, 236, 162, 132, 230, 186, 142, 170, 163, 234, 68, 79, 48, 140, 141, 239, 107, 263, 246}

// ModelTraffic calls FeatureService the way a dispatch model would: a few
// zones per request, mostly busy ones, through the default bounded-staleness
// mode with a 100 ms deadline. It keeps the serving panels of the live
// dashboard populated; it is not a benchmark (see cmd/loadgen for that).
func ModelTraffic(ctx context.Context, addr string, qps float64, log *slog.Logger) {
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("model traffic", "err", err)
		return
	}
	defer cc.Close()
	cli := sfv1.NewFeatureServiceClient(cc)
	t := time.NewTicker(time.Duration(float64(time.Second) / qps))
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		ids := make([]string, 0, 3)
		for i := 0; i < 3; i++ {
			z := busyZones[rand.Intn(len(busyZones))]
			if rand.Intn(4) == 0 {
				z = 1 + rand.Intn(263)
			}
			ids = append(ids, strconv.Itoa(z))
		}
		go func() {
			rctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancel()
			_, _ = cli.GetFeatures(rctx, &sfv1.GetFeaturesRequest{FeatureView: "zone_demand_v1", EntityIds: ids})
		}()
	}
}
