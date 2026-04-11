package proxy

import "github.com/xtls/xray-core/common/signal"

type activityAware interface {
	SetActivity(signal.ActivityUpdater)
}

func BindActivity(timer signal.ActivityUpdater, values ...any) {
	for _, value := range values {
		if aware, ok := value.(activityAware); ok {
			aware.SetActivity(timer)
		}
	}
}
