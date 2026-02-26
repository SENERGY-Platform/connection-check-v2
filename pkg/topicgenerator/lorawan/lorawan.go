package lorawan

import (
	"github.com/SENERGY-Platform/connection-check-v2/pkg/configuration"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/model"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/topicgenerator/common"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/topicgenerator/known"
)

func init() {
	known.Generators["lorawan"] = TopicGenerator
}

func TopicGenerator(_ configuration.Config, _ common.DeviceTypeProvider, _ model.ExtendedDevice) (topicCandidates []string, err error) {
	return nil, common.NoSubscriptionExpected
}
