package model

type SearchData struct {
	Id        string  `json:"id"`
	TopicName string  `json:"topic_name"`
	Value     *string `json:"value"`
	Key       *string `json:"key"`
}
