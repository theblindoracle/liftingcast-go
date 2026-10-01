package liftingcast

// LiftName represents the type of lift
type LiftName string

const (
	LiftSquat LiftName = "squat"
	LiftBench LiftName = "bench"
	LiftDead  LiftName = "dead"
)

// AttemptNumber represents the attempt number
type AttemptNumber string

const (
	Attempt1 AttemptNumber = "1"
	Attempt2 AttemptNumber = "2"
	Attempt3 AttemptNumber = "3"
	Attempt4 AttemptNumber = "4"
)

// ClockState represents the state of the platform clock
type ClockState string

const (
	ClockInitial ClockState = "initial"
	ClockStarted ClockState = "started"
)

// RecordData represents a record that may be broken
type RecordData struct {
	Gender          *string `json:"gender"`
	EquipmentLevel  *string `json:"equipmentLevel"`
	Tested          *string `json:"tested"` // "U" | "T" | null
	DivisionCode    *string `json:"divisionCode"`
	WeightClass     *string `json:"weightClass"`
	CompetitionType *string `json:"competitionType"` // "FP" | "PP" | "SL" | null
	Lift            string  `json:"lift"`            // "S" | "B" | "D" | "T"
	Location        *string `json:"location"`
	RecordWeight    float64 `json:"recordWeight"`
}

// RefDecisionCards represents referee cards
type RefDecisionCards struct {
	Red    *bool `json:"red,omitempty"`
	Blue   *bool `json:"blue,omitempty"`
	Yellow *bool `json:"yellow,omitempty"`
}

// RefDecision represents a referee's decision
type RefDecision struct {
	Decision *string           `json:"decision,omitempty"` // "bad" | "good" | null
	Cards    *RefDecisionCards `json:"cards,omitempty"`
}

// RefDecisions represents decisions from all three referees
type RefDecisions struct {
	Left  *RefDecision `json:"left,omitempty"`
	Head  *RefDecision `json:"head,omitempty"`
	Right *RefDecision `json:"right,omitempty"`
}

// LifterAttempt represents a single attempt by a lifter
type LifterAttempt struct {
	ID        *string         `json:"id"`
	Weight    NullableFloat64 `json:"weight"`
	Result    *string         `json:"result"`
	Records   []RecordData    `json:"records"`
	Decisions *RefDecisions   `json:"decisions"`
}

// LifterAttempts maps attempt numbers to attempts
type LifterAttempts map[AttemptNumber]LifterAttempt

// LifterLifts maps lift names to attempts
type LifterLifts map[LiftName]LifterAttempts

// LifterDivision represents a lifter's division and placing
type LifterDivision struct {
	DivisionID      *string         `json:"divisionId"`
	WeightClassID   *string         `json:"weightClassId"`
	Score           interface{}     `json:"score"` // string | number | null
	ForecastedScore interface{}     `json:"forecastedScore"`
	Place           *int            `json:"place"`
	ForecastedPlace *int            `json:"forecastedPlace"`
	Total           NullableFloat64 `json:"total"`
	ForecastedTotal NullableFloat64 `json:"forecastedTotal"`
}

// Lifter represents a competitor in the meet
type Lifter struct {
	ID              string           `json:"id"`
	MemberNumber    *string          `json:"memberNumber"`
	Name            *string          `json:"name"`
	Gender          *string          `json:"gender"`
	Team            *string          `json:"team"`
	State           *string          `json:"state"`
	Country         *string          `json:"country"`
	BodyWeight      NullableFloat64  `json:"bodyWeight"`
	Lot             *int             `json:"lot"`
	Session         interface{}      `json:"session"` // number | string | null
	Flight          *string          `json:"flight"`
	SquatRackHeight *string          `json:"squatRackHeight"`
	BenchRackHeight *string          `json:"benchRackHeight"`
	Platform        *string          `json:"platform"`
	Divisions       []LifterDivision `json:"divisions"`
	Lifts           LifterLifts      `json:"lifts"`
}

// Lifters maps lifter IDs to lifters
type Lifters map[string]Lifter

// Attempt represents a basic attempt structure
type Attempt struct {
	ID            string        `json:"id"`
	LiftName      LiftName      `json:"liftName"`
	AttemptNumber AttemptNumber `json:"attemptNumber"`
	Lifter        struct {
		ID string `json:"id"`
	} `json:"lifter"`
}

// CurrentAttempt extends Attempt with if-successful predictions
type CurrentAttempt struct {
	Attempt
	IfSuccessfulPlaces map[string]int     `json:"ifSuccessfulPlaces,omitempty"`
	IfSuccessfulScores map[string]float64 `json:"ifSuccessfulScores,omitempty"`
}

// RefLight represents a referee's light
type RefLight struct {
	Decision *string           `json:"decision,omitempty"` // "good" | "bad" | null
	Cards    *RefDecisionCards `json:"cards,omitempty"`
}

// RefLights represents lights from all three referees
type RefLights struct {
	Left  RefLight `json:"left"`
	Head  RefLight `json:"head"`
	Right RefLight `json:"right"`
}

// PlatformData represents a platform's state
type PlatformData struct {
	ID                  string          `json:"id"`
	Name                *string         `json:"name"`
	ClockState          *ClockState     `json:"clockState"`
	ClockTimerLength    *int            `json:"clockTimerLength"`
	BarAndCollarsWeight NullableFloat64 `json:"barAndCollarsWeight"`
	CurrentAttempt      *CurrentAttempt `json:"currentAttempt"`
	NextAttempts        []Attempt       `json:"nextAttempts"`
	RefLights           RefLights       `json:"refLights"`
}

// Platforms maps platform IDs to platform data
type Platforms map[string]PlatformData

// WeightClassData represents a weight class
type WeightClassData struct {
	Name      *string         `json:"name"`
	MaxWeight NullableFloat64 `json:"maxWeight"`
}

// WeightClasses maps weight class IDs to weight class data
type WeightClasses map[string]WeightClassData

// DivisionData represents a division
type DivisionData struct {
	ID            string        `json:"id"`
	Name          *string       `json:"name"`
	ScoreBy       *string       `json:"scoreBy"`
	WeightClasses WeightClasses `json:"weightClasses"`
}

// Divisions maps division IDs to division data
type Divisions map[string]DivisionData

// TeamData represents a team's standing
type TeamData struct {
	Place  int     `json:"place"`
	Points float64 `json:"points"`
}

// Teams maps team names to team standings
type Teams map[string]TeamData

// MeetState represents the complete meet state from LiftingCast API
type MeetState struct {
	Name       string     `json:"name"`
	Units      *string    `json:"units"` // "KG" | "LBS" | null
	Federation string     `json:"federation"`
	Lifters    *Lifters   `json:"lifters,omitempty"`
	Platforms  *Platforms `json:"platforms,omitempty"`
	Divisions  *Divisions `json:"divisions,omitempty"`
	Teams      *Teams     `json:"teams,omitempty"`
}
