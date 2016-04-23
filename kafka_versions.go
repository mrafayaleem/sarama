package sarama

import (
	"github.com/blang/semver"
)

// Note: Kafka versions are X.0.1 instead of 0.X.0.1 because the library used  for version parsing is based on
// semantic versioning. It is highly recommended to use versions from here instead of creating your own versions.

var LatestStable, _ = semver.New("9.0.1")
var V0_10_0, _ = semver.New("10.0.0")
var V0_9_0_1 = LatestStable
var V0_9_0_0, _ = semver.New("9.0.0")
var V0_8_2_2, _ = semver.New("8.2.2")
var V0_8_2_1, _ = semver.New("8.2.1")
var V0_8_2_0, _ = semver.New("8.2.0")
