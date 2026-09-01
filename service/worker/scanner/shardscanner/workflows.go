package shardscanner

import "go.uber.org/cadence/activity"

func init() {
	// shardscanner activities
	activity.RegisterWithOptions(scannerEmitMetricsActivity, activity.RegisterOptions{Name: ActivityScannerEmitMetrics})
	activity.RegisterWithOptions(scanShardActivity, activity.RegisterOptions{Name: ActivityScanShard})
	activity.RegisterWithOptions(scannerConfigActivity, activity.RegisterOptions{Name: ActivityScannerConfig})
	activity.RegisterWithOptions(fixerConfigActivity, activity.RegisterOptions{Name: ActivityFixerConfig})
	activity.RegisterWithOptions(fixerCorruptedKeysActivity, activity.RegisterOptions{Name: ActivityFixerCorruptedKeys})
	activity.RegisterWithOptions(fixShardActivity, activity.RegisterOptions{Name: ActivityFixShard})
}
