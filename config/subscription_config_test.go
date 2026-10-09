package config

import (
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestSubscriptionIdleConfiguration(t *testing.T) {
	for _, interval := range []string{"1s", "250ms", "0s", "-1s"} {
		t.Run(interval, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)
			t.Setenv("ORISUN_SUBSCRIPTION_IDLE_THRESHOLD", interval)
			cfg, err := LoadConfig()
			if interval == "0s" || interval == "-1s" {
				require.ErrorContains(t, err, "ORISUN_SUBSCRIPTION_IDLE_THRESHOLD")
				return
			}
			require.NoError(t, err)
			want, err := time.ParseDuration(interval)
			require.NoError(t, err)
			require.Equal(t, want, cfg.SubscriptionIdleThreshold)
		})
	}
}
