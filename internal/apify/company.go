package apify

// todo: after getting more responses, check for what is nullable and not zero valued
type Company struct {
	InputIdentifier string           `json:"input_identifier" bson:"input_identifier"`
	BasicInfo       CompanyBasicInfo `json:"basic_info" bson:"basic_info"`
	Stats           Stats            `json:"stats" bson:"stats"`
	Locations       CompanyLocations `json:"locations" bson:"locations"`
	Media           Media            `json:"media" bson:"media"`
	CompanyURN      string           `json:"company_urn" bson:"company_urn"`
}

type CompanyBasicInfo struct {
	Name          string       `json:"name" bson:"name"`
	UniversalName string       `json:"universal_name" bson:"universal_name"`
	Description   string       `json:"description" bson:"description"`
	Website       string       `json:"webite" bson:"website"`
	LinkedinURL   string       `json:"linkedin_url" bson:"linkedin_url"`
	Specialties   []string     `json:"specialties" bson:"specialties"`
	Industries    []string     `json:"industries" bson:"industries"`
	IsVerified    bool         `json:"is_verified" bson:"is_verified"`
	FoundedInfo   FoundedDate  `json:"founded_info" bson:"founded_info"`
	PageType      string       `json:"page_type" bson:"page_type"` // COMPANY or SCHOOL
	Verification  Verification `json:"verification" bson:"verification"`
}

type FoundedDate struct {
	Year  int     `json:"year" bson:"year"`   // 2012
	Month *string `json:"month" bson:"month"` // Jun
	Day   *any    `json:"day" bson:"day"`     // unsure whether data type is int or string yet
}

type Verification struct {
	IsVerified     bool   `json:"is_verified" bson:"is_verified"`
	LastVerifiedAt *int64 `json:"last_verified_at" bson:"last_verified_at"`
}

type EmployeeCountRange struct {
	Start int `json:"start" bson:"start"`
	End   int `json:"end" bson:"end"`
}

type Stats struct {
	EmployeeCount      int                `json:"employee_count" bson:"employee_count"`
	FollowerCount      int                `json:"follower_count" bson:"follower_count"`
	EmployeeCountRange EmployeeCountRange `json:"employee_count_range" bson:"employee_count_range"`
	StudentCount       *int               `json:"student_count" bson:"student_count"`
}

type LocationCompany struct {
	Country     string  `json:"country" bson:"country"`
	State       string  `json:"state" bson:"state"`
	City        string  `json:"city" bson:"city"`
	PostalCode  string  `json:"postal_code" bson:"postal_code"`
	Line1       *string `json:"line1" bson:"line1"`
	Line2       *string `json:"line2" bson:"line2"`
	IsHQ        bool    `json:"is_hq" bson:"is_hq"`
	Description *string `json:"description" bson:"description"`
	Region      *string `json:"region,omitempty" bson:"region"` // used in offices but not headquarters
}

type LocationGeo struct {
	Latitude  float64 `json:"latitude" bson:"latitude"`
	Longitude float64 `json:"longitude" bson:"longitude"`
}

type CompanyLocations struct {
	Headquarters   LocationCompany   `json:"headquarters" bson:"headquarters"`
	Offices        []LocationCompany `json:"offices" bson:"offices"`
	GeoCoordinates LocationGeo       `json:"geo_cooridinates" bson:"geo_cooridinates"`
}

type Media struct {
	LogoURL         string `json:"logo_url" bson:"logo_url"`
	CoverURL        string `json:"cover_url" bson:"cover_url"`
	CroppedCoverURL string `json:"cropped_cover_url" bson:"cropped_cover_url"`
}

type Funding struct {
	TotalRounds   *int               `json:"total_rounds" bson:"total_rounds"`
	LatestRound   LatestFundingRound `json:"latest_round" bson:"latest_rounds"`
	CrunchbaseURL string             `json:"crunchbase_url" bson:"crunchbase_url"`
}

type LatestFundingRound struct {
	Type           string `json:"type" bson:"type"`
	Date           *Date  `json:"date" bson:"date"`
	URL            string `json:"url" bson:"url"`
	InvestorsCount *int   `json:"investors_count" bson:"investors_count"`
}

type Links struct {
	Website        string  `json:"website" bson:"website"`
	Linkedin       string  `json:"linkedin" bson:"linkedin"`
	JobSearch      string  `json:"job_search" bson:"job_search"`
	SalesNavigator *string `json:"sales_navigator" bson:"sales_navigator"`
	Crunchbase     string  `json:"crunchbase" bson:"crunchbase"`
}
