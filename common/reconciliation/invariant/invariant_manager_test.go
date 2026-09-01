package invariant

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/reconciliation/entity"
)

type InvariantManagerSuite struct {
	*require.Assertions
	suite.Suite
	controller *gomock.Controller
}

func TestInvariantManagerSuite(t *testing.T) {
	suite.Run(t, new(InvariantManagerSuite))
}

func (s *InvariantManagerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
}

func (s *InvariantManagerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *InvariantManagerSuite) TestRunChecks() {
	testCases := []struct {
		checkResults []CheckResult
		expected     ManagerCheckResult
	}{
		{
			checkResults: nil,
			expected: ManagerCheckResult{
				CheckResultType: CheckResultTypeHealthy,
				CheckResults:    nil,
			},
		},
		{
			checkResults: []CheckResult{
				{
					CheckResultType: CheckResultTypeHealthy,
					InvariantName:   Name("first"),
					Info:            "invariant 1 info",
					InfoDetails:     "invariant 1 info details",
				},
				{
					CheckResultType: CheckResultTypeFailed,
					InvariantName:   Name("second"),
					Info:            "invariant 2 info",
					InfoDetails:     "invariant 2 info details",
				},
			},
			expected: ManagerCheckResult{
				CheckResultType:          CheckResultTypeFailed,
				DeterminingInvariantType: NamePtr("second"),
				CheckResults: []CheckResult{
					{
						CheckResultType: CheckResultTypeHealthy,
						InvariantName:   Name("first"),
						Info:            "invariant 1 info",
						InfoDetails:     "invariant 1 info details",
					},
					{
						CheckResultType: CheckResultTypeFailed,
						InvariantName:   Name("second"),
						Info:            "invariant 2 info",
						InfoDetails:     "invariant 2 info details",
					},
				},
			},
		},
		{
			checkResults: []CheckResult{
				{
					CheckResultType: CheckResultTypeHealthy,
					InvariantName:   Name("first"),
					Info:            "invariant 1 info",
					InfoDetails:     "invariant 1 info details",
				},
				{
					CheckResultType: CheckResultTypeCorrupted,
					InvariantName:   Name("second"),
					Info:            "invariant 2 info",
					InfoDetails:     "invariant 2 info details",
				},
			},
			expected: ManagerCheckResult{
				CheckResultType:          CheckResultTypeCorrupted,
				DeterminingInvariantType: NamePtr("second"),
				CheckResults: []CheckResult{
					{
						CheckResultType: CheckResultTypeHealthy,
						InvariantName:   Name("first"),
						Info:            "invariant 1 info",
						InfoDetails:     "invariant 1 info details",
					},
					{
						CheckResultType: CheckResultTypeCorrupted,
						InvariantName:   Name("second"),
						Info:            "invariant 2 info",
						InfoDetails:     "invariant 2 info details",
					},
				},
			},
		},
		{
			checkResults: []CheckResult{
				{
					CheckResultType: CheckResultTypeHealthy,
					InvariantName:   Name("first"),
					Info:            "invariant 1 info",
					InfoDetails:     "invariant 1 info details",
				},
				{
					CheckResultType: CheckResultTypeHealthy,
					InvariantName:   Name("second"),
					Info:            "invariant 2 info",
					InfoDetails:     "invariant 2 info details",
				},
			},
			expected: ManagerCheckResult{
				CheckResultType:          CheckResultTypeHealthy,
				DeterminingInvariantType: nil,
				CheckResults: []CheckResult{
					{
						CheckResultType: CheckResultTypeHealthy,
						InvariantName:   Name("first"),
						Info:            "invariant 1 info",
						InfoDetails:     "invariant 1 info details",
					},
					{
						CheckResultType: CheckResultTypeHealthy,
						InvariantName:   Name("second"),
						Info:            "invariant 2 info",
						InfoDetails:     "invariant 2 info details",
					},
				},
			},
		},
		{
			checkResults: []CheckResult{
				{
					CheckResultType: CheckResultTypeHealthy,
					InvariantName:   Name("first"),
					Info:            "invariant 1 info",
					InfoDetails:     "invariant 1 info details",
				},
				{
					CheckResultType: CheckResultTypeCorrupted,
					InvariantName:   Name("second"),
					Info:            "invariant 2 info",
					InfoDetails:     "invariant 2 info details",
				},
				{
					CheckResultType: CheckResultTypeFailed,
					InvariantName:   Name("third"),
					Info:            "invariant 3 info",
					InfoDetails:     "invariant 3 info details",
				},
				{
					CheckResultType: CheckResultTypeHealthy,
					InvariantName:   Name("forth"),
					Info:            "invariant 4 info",
					InfoDetails:     "invariant 4 info details",
				},
			},
			expected: ManagerCheckResult{
				CheckResultType:          CheckResultTypeFailed,
				DeterminingInvariantType: NamePtr("third"),
				CheckResults: []CheckResult{
					{
						CheckResultType: CheckResultTypeHealthy,
						InvariantName:   Name("first"),
						Info:            "invariant 1 info",
						InfoDetails:     "invariant 1 info details",
					},
					{
						CheckResultType: CheckResultTypeCorrupted,
						InvariantName:   Name("second"),
						Info:            "invariant 2 info",
						InfoDetails:     "invariant 2 info details",
					},
					{
						CheckResultType: CheckResultTypeFailed,
						InvariantName:   Name("third"),
						Info:            "invariant 3 info",
						InfoDetails:     "invariant 3 info details",
					},
					{
						CheckResultType: CheckResultTypeHealthy,
						InvariantName:   Name("forth"),
						Info:            "invariant 4 info",
						InfoDetails:     "invariant 4 info details",
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		invariants := make([]Invariant, len(tc.checkResults))
		for i := 0; i < len(tc.checkResults); i++ {
			mockInvariant := NewMockInvariant(s.controller)
			mockInvariant.EXPECT().Check(gomock.Any(), gomock.Any()).Return(tc.checkResults[i])
			invariants[i] = mockInvariant
		}
		manager := &invariantManager{
			invariants: invariants,
		}
		s.Equal(tc.expected, manager.RunChecks(context.Background(), entity.Execution{}))
	}
}

func (s *InvariantManagerSuite) TestRunFixes() {
	testCases := []struct {
		fixResults []FixResult
		expected   ManagerFixResult
	}{
		{
			fixResults: nil,
			expected: ManagerFixResult{
				FixResultType:            FixResultTypeSkipped,
				DeterminingInvariantName: nil,
				FixResults:               nil,
			},
		},
		{
			fixResults: []FixResult{
				{
					FixResultType: FixResultTypeFixed,
					InvariantName: Name("first"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeCorrupted,
						Info:            "invariant 1 check info",
						InfoDetails:     "invariant 1 check info details",
					},
					Info:        "invariant 1 info",
					InfoDetails: "invariant 1 info details",
				},
				{
					FixResultType: FixResultTypeFailed,
					InvariantName: Name("second"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeCorrupted,
						Info:            "invariant 2 check info",
						InfoDetails:     "invariant 2 check info details",
					},
					Info:        "invariant 2 info",
					InfoDetails: "invariant 2 info details",
				},
			},
			expected: ManagerFixResult{
				FixResultType:            FixResultTypeFailed,
				DeterminingInvariantName: NamePtr("second"),
				FixResults: []FixResult{
					{
						FixResultType: FixResultTypeFixed,
						InvariantName: Name("first"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeCorrupted,
							Info:            "invariant 1 check info",
							InfoDetails:     "invariant 1 check info details",
						},
						Info:        "invariant 1 info",
						InfoDetails: "invariant 1 info details",
					},
					{
						FixResultType: FixResultTypeFailed,
						InvariantName: Name("second"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeCorrupted,
							Info:            "invariant 2 check info",
							InfoDetails:     "invariant 2 check info details",
						},
						Info:        "invariant 2 info",
						InfoDetails: "invariant 2 info details",
					},
				},
			},
		},
		{
			fixResults: []FixResult{
				{
					FixResultType: FixResultTypeSkipped,
					InvariantName: Name("first"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeHealthy,
						Info:            "invariant 1 check info",
						InfoDetails:     "invariant 1 check info details",
					},
					Info:        "invariant 1 info",
					InfoDetails: "invariant 1 info details",
				},
				{
					FixResultType: FixResultTypeSkipped,
					InvariantName: Name("second"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeHealthy,
						Info:            "invariant 2 check info",
						InfoDetails:     "invariant 2 check info details",
					},
					Info:        "invariant 2 info",
					InfoDetails: "invariant 2 info details",
				},
			},
			expected: ManagerFixResult{
				FixResultType:            FixResultTypeSkipped,
				DeterminingInvariantName: nil,
				FixResults: []FixResult{
					{
						FixResultType: FixResultTypeSkipped,
						InvariantName: Name("first"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeHealthy,
							Info:            "invariant 1 check info",
							InfoDetails:     "invariant 1 check info details",
						},
						Info:        "invariant 1 info",
						InfoDetails: "invariant 1 info details",
					},
					{
						FixResultType: FixResultTypeSkipped,
						InvariantName: Name("second"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeHealthy,
							Info:            "invariant 2 check info",
							InfoDetails:     "invariant 2 check info details",
						},
						Info:        "invariant 2 info",
						InfoDetails: "invariant 2 info details",
					},
				},
			},
		},
		{
			fixResults: []FixResult{
				{
					FixResultType: FixResultTypeSkipped,
					InvariantName: Name("first"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeHealthy,
						Info:            "invariant 1 check info",
						InfoDetails:     "invariant 1 check info details",
					},
					Info:        "invariant 1 info",
					InfoDetails: "invariant 1 info details",
				},
				{
					FixResultType: FixResultTypeFixed,
					InvariantName: Name("second"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeCorrupted,
						Info:            "invariant 2 check info",
						InfoDetails:     "invariant 2 check info details",
					},
					Info:        "invariant 2 info",
					InfoDetails: "invariant 2 info details",
				},
				{
					FixResultType: FixResultTypeSkipped,
					InvariantName: Name("third"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeHealthy,
						Info:            "invariant 3 check info",
						InfoDetails:     "invariant 3 check info details",
					},
					Info:        "invariant 3 info",
					InfoDetails: "invariant 3 info details",
				},
			},
			expected: ManagerFixResult{
				FixResultType:            FixResultTypeFixed,
				DeterminingInvariantName: NamePtr("second"),
				FixResults: []FixResult{
					{
						FixResultType: FixResultTypeSkipped,
						InvariantName: Name("first"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeHealthy,
							Info:            "invariant 1 check info",
							InfoDetails:     "invariant 1 check info details",
						},
						Info:        "invariant 1 info",
						InfoDetails: "invariant 1 info details",
					},
					{
						FixResultType: FixResultTypeFixed,
						InvariantName: Name("second"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeCorrupted,
							Info:            "invariant 2 check info",
							InfoDetails:     "invariant 2 check info details",
						},
						Info:        "invariant 2 info",
						InfoDetails: "invariant 2 info details",
					},
					{
						FixResultType: FixResultTypeSkipped,
						InvariantName: Name("third"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeHealthy,
							Info:            "invariant 3 check info",
							InfoDetails:     "invariant 3 check info details",
						},
						Info:        "invariant 3 info",
						InfoDetails: "invariant 3 info details",
					},
				},
			},
		},
		{
			fixResults: []FixResult{
				{
					FixResultType: FixResultTypeSkipped,
					InvariantName: Name("first"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeHealthy,
						Info:            "invariant 1 check info",
						InfoDetails:     "invariant 1 check info details",
					},
					Info:        "invariant 1 info",
					InfoDetails: "invariant 1 info details",
				},
				{
					FixResultType: FixResultTypeFixed,
					InvariantName: Name("second"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeCorrupted,
						Info:            "invariant 2 check info",
						InfoDetails:     "invariant 2 check info details",
					},
					Info:        "invariant 2 info",
					InfoDetails: "invariant 2 info details",
				},
				{
					FixResultType: FixResultTypeSkipped,
					InvariantName: Name("third"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeHealthy,
						Info:            "invariant 3 check info",
						InfoDetails:     "invariant 3 check info details",
					},
					Info:        "invariant 3 info",
					InfoDetails: "invariant 3 info details",
				},
				{
					FixResultType: FixResultTypeFailed,
					InvariantName: Name("forth"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeCorrupted,
						Info:            "invariant 4 check info",
						InfoDetails:     "invariant 2 check info details",
					},
					Info:        "invariant 4 info",
					InfoDetails: "invariant 4 info details",
				},
			},
			expected: ManagerFixResult{
				FixResultType:            FixResultTypeFailed,
				DeterminingInvariantName: NamePtr("forth"),
				FixResults: []FixResult{
					{
						FixResultType: FixResultTypeSkipped,
						InvariantName: Name("first"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeHealthy,
							Info:            "invariant 1 check info",
							InfoDetails:     "invariant 1 check info details",
						},
						Info:        "invariant 1 info",
						InfoDetails: "invariant 1 info details",
					},
					{
						FixResultType: FixResultTypeFixed,
						InvariantName: Name("second"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeCorrupted,
							Info:            "invariant 2 check info",
							InfoDetails:     "invariant 2 check info details",
						},
						Info:        "invariant 2 info",
						InfoDetails: "invariant 2 info details",
					},
					{
						FixResultType: FixResultTypeSkipped,
						InvariantName: Name("third"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeHealthy,
							Info:            "invariant 3 check info",
							InfoDetails:     "invariant 3 check info details",
						},
						Info:        "invariant 3 info",
						InfoDetails: "invariant 3 info details",
					},
					{
						FixResultType: FixResultTypeFailed,
						InvariantName: Name("forth"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeCorrupted,
							Info:            "invariant 4 check info",
							InfoDetails:     "invariant 2 check info details",
						},
						Info:        "invariant 4 info",
						InfoDetails: "invariant 4 info details",
					},
				},
			},
		},
		{
			fixResults: []FixResult{
				{
					FixResultType: FixResultTypeSkipped,
					InvariantName: Name("first"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeHealthy,
						Info:            "invariant 1 check info",
						InfoDetails:     "invariant 1 check info details",
					},
					Info:        "invariant 1 info",
					InfoDetails: "invariant 1 info details",
				},
				{
					FixResultType: FixResultTypeFailed,
					InvariantName: Name("second"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeCorrupted,
						Info:            "invariant 4 check info",
						InfoDetails:     "invariant 2 check info details",
					},
					Info:        "invariant 4 info",
					InfoDetails: "invariant 4 info details",
				},
				{
					FixResultType: FixResultTypeFixed,
					InvariantName: Name("third"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeCorrupted,
						Info:            "invariant 2 check info",
						InfoDetails:     "invariant 2 check info details",
					},
					Info:        "invariant 2 info",
					InfoDetails: "invariant 2 info details",
				},
				{
					FixResultType: FixResultTypeSkipped,
					InvariantName: Name("forth"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeHealthy,
						Info:            "invariant 3 check info",
						InfoDetails:     "invariant 3 check info details",
					},
					Info:        "invariant 3 info",
					InfoDetails: "invariant 3 info details",
				},
			},
			expected: ManagerFixResult{
				FixResultType:            FixResultTypeFailed,
				DeterminingInvariantName: NamePtr("second"),
				FixResults: []FixResult{
					{
						FixResultType: FixResultTypeSkipped,
						InvariantName: Name("first"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeHealthy,
							Info:            "invariant 1 check info",
							InfoDetails:     "invariant 1 check info details",
						},
						Info:        "invariant 1 info",
						InfoDetails: "invariant 1 info details",
					},
					{
						FixResultType: FixResultTypeFailed,
						InvariantName: Name("second"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeCorrupted,
							Info:            "invariant 4 check info",
							InfoDetails:     "invariant 2 check info details",
						},
						Info:        "invariant 4 info",
						InfoDetails: "invariant 4 info details",
					},
					{
						FixResultType: FixResultTypeFixed,
						InvariantName: Name("third"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeCorrupted,
							Info:            "invariant 2 check info",
							InfoDetails:     "invariant 2 check info details",
						},
						Info:        "invariant 2 info",
						InfoDetails: "invariant 2 info details",
					},
					{
						FixResultType: FixResultTypeSkipped,
						InvariantName: Name("forth"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeHealthy,
							Info:            "invariant 3 check info",
							InfoDetails:     "invariant 3 check info details",
						},
						Info:        "invariant 3 info",
						InfoDetails: "invariant 3 info details",
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		invariants := make([]Invariant, len(tc.fixResults))
		for i := 0; i < len(tc.fixResults); i++ {
			mockInvariant := NewMockInvariant(s.controller)
			mockInvariant.EXPECT().Fix(gomock.Any(), gomock.Any()).Return(tc.fixResults[i])
			invariants[i] = mockInvariant
		}
		manager := &invariantManager{
			invariants: invariants,
		}
		s.Equal(tc.expected, manager.RunFixes(context.Background(), entity.Execution{}))
	}
}
