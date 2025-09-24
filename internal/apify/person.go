package apify

// todo: after getting more responses, check for what is nullable and not zero valued
type Person struct {
	BasicInfo  PersonBasicInfo `json:"basic_info" bson:"basic_info"`
	Experience []Experience    `json:"experience" bson:"experience"`
	Education  []Education     `json:"education" bson:"education"`
	ProfileURL string          `json:"profile_url" bson:"profile_url"`
}

type LocationPerson struct {
	Country     string `json:"country" bson:"country"`
	City        string `json:"city" bson:"city"`
	Full        string `json:"full" bson:"full"`
	CountryCode string `json:"country_code" bson:"country_code"`
}

type PersonBasicInfo struct {
	FullName             string         `json:"full_name" bson:"full_name"`
	FirstName            string         `json:"first_name" bson:"first_name"`
	LastName             string         `json:"last_name" bson:"last_name"`
	Headline             string         `json:"headline" bson:"headline"`
	ProfileURL           string         `json:"profile_url" bson:"profile_url"`
	PublicIdentifer      string         `json:"public_identifier" bson:"public_identifier"`
	ProfilePictureURL    string         `json:"profile_picture_url" bson:"profile_picture_url"`
	About                string         `json:"about" bson:"about"`
	Location             LocationPerson `json:"location" bson:"location"`
	CreatorHashtags      []string       `json:"creator_hashtags" bson:"creator_hashtags"`
	IsCreator            bool           `json:"is_creator" bson:"is_creator"`
	IsInfluencer         bool           `json:"is_influencer" bson:"is_influencer"`
	IsPremium            bool           `json:"is_premium" bson:"is_premium"`
	CreatedTimestamp     int64          `json:"created_timestamp" bson:"created_timestamp"`
	ShowFollerCount      bool           `json:"show_follower_count" bson:"show_follower_count"`
	BackgroundPictureURL string         `json:"background_picture_url" bson:"background_picture_url"`
	URN                  string         `json:"urn" bson:"urn"`
	FollowerCount        int            `json:"follower_count" bson:"follower_count"`
	ConnectionCount      int            `json:"connection_count" bson:"connection_count"`
	CurrentCompany       string         `json:"current_company" bson:"current_company"`
	CurrentCompanyURN    string         `json:"current_company_urn" bson:"current_company_urn"`
	CurrentCompanyURL    string         `json:"current_company_url" bson:"current_company_url"`
}

type Date struct {
	Year  int     `json:"year" bson:"year"`   // 2012
	Month *string `json:"month" bson:"month"` // Jun
}

type Experience struct {
	Title              string   `json:"title" bson:"title"`
	Company            string   `json:"company" bson:"company"`
	Location           string   `json:"location" bson:"location"` // format: Greater Boston
	Description        string   `json:"description" bson:"description"`
	Duration           string   `json:"duration" bson:"duration"` // format: Jun 2012 - Present · 13 yrs 3 mos
	StartDate          Date     `json:"start_date" bson:"start_date"`
	EndDate            Date     `json:"end_date" bson:"end_date"`
	IsCurrent          bool     `json:"is_current" bson:"is_current"`
	CompanyLinkedinURL string   `json:"company_linkedin_url" bson:"company_linkedin_url"`
	CompanyLogoURL     string   `json:"company_logo_url" bson:"company_logo_url"`
	Skills             []string `json:"skills" bson:"skills"`
	CompanyID          string   `json:"company_id" bson:"company_id"`
	SkillsURL          string   `json:"skills_url" bosn:"skills_url"`
}

type Education struct {
	School            string `json:"school" bson:"school"`
	Degree            string `json:"degree" bson:"degree"`
	DegreeName        string `json:"degree_name" bson:"degree_name"`
	FieldOfStudy      string `json:"field_of_study" bson:"field_of_study"`
	Duration          string `json:"duraction" bson:"duraction"`
	StartDate         Date   `json:"start_date" bson:"start_date"`
	EndDate           Date   `json:"end_date" bson:"end_date"`
	SchoolLinkedinURL string `json:"school_linkedin_url" bson:"school_linkedin_url"`
	SchoolID          string `json:"school_id" bson:"school_id"`
}
