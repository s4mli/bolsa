package job

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func testNotificationPayload(t *testing.T) {
	a := Telegram{
		Retries:       1,
		Email:         "sam@ecal.com",
		CallbackURL:   "http://127.0.0.1",
		CustomFields:  nil,
		RelevantNames: []string{"name1", "name2"},
	}
	assert.Equal(t, map[string]interface{}{
		"email":         "sam@ecal.com",
		"relevantNames": []string{"name1", "name2"},
	}, a.notificationPayload())

	a.Email = ""
	assert.Equal(t, map[string]interface{}{
		"relevantNames": []string{"name1", "name2"},
	}, a.notificationPayload())

	a.CustomFields = map[string]interface{}{
		"country_code": "AU",
		"country_name": "Australia",
		"state_code":   "VIC",
		"state_name":   "Victoria",
		"zip_code":     "3004",
		"city_name":    "Melbourne",
		"latitude":     -37.814,
		"longitude":    144.9633,
	}
	assert.Equal(t, map[string]interface{}{
		"relevantNames": []string{"name1", "name2"},
		"customFields": map[string]interface{}{
			"country_code": "AU",
			"country_name": "Australia",
			"state_code":   "VIC",
			"state_name":   "Victoria",
			"zip_code":     "3004",
			"city_name":    "Melbourne",
			"latitude":     -37.814,
			"longitude":    144.9633,
		},
	}, a.notificationPayload())

	a.CustomFields = []map[string]interface{}{
		{
			"name":  "Favourite Team",
			"value": "Manchester United",
		},
		{
			"name":  "Date of Birth",
			"value": "1979-08-12",
		},
		{
			"name":  "Over 13 yrs",
			"value": "Yes",
		},
		{
			"name":  "Country Code",
			"value": "sd",
		},
	}

	assert.Equal(t, map[string]interface{}{
		"relevantNames": []string{"name1", "name2"},
		"customFields": []map[string]interface{}{
			{
				"name":  "Favourite Team",
				"value": "Manchester United",
			},
			{
				"name":  "Date of Birth",
				"value": "1979-08-12",
			},
			{
				"name":  "Over 13 yrs",
				"value": "Yes",
			},
			{
				"name":  "Country Code",
				"value": "sd",
			},
		},
	}, a.notificationPayload())
}

func TestTelegram(t *testing.T) {
	testNotificationPayload(t)
}
