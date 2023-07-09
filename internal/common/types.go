package common

type Response struct {
	StatusCode int    `json:"StatusCode"`
	Err        error  `json:"err"`
	SelfLink   string `json:"selfLink"`
}
