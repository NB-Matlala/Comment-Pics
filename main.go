package main

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/gocolly/colly/v2"
)

type Property struct {
	ListingID      string `json:"Listing ID"`
	ErfSize        string `json:"Erf Size"`
	PropertyType   string `json:"Property Type"`
	FloorSize      string `json:"Floor Size"`
	RatesAndTaxes  string `json:"Rates and taxes"`
	Levies         string `json:"Levies"`
	Bedrooms       string `json:"Bedrooms"`
	Bathrooms      string `json:"Bathrooms"`
	Lounges        string `json:"Lounges"`
	Dining         string `json:"Dining"`
	Garages        string `json:"Garages"`
	CoveredParking string `json:"Covered Parking"`
	Storeys        string `json:"Storeys"`
	AgentName      string `json:"Agent name"`
	AgentURL       string `json:"Agent Url"`
}

func getPages(soup string) int {
	re := regexp.MustCompile(`(\d+) results`)
	matches := re.FindStringSubmatch(soup)
	if len(matches) > 1 {
		numResults := matches[1]
		results, _ := strconv.Atoi(numResults)
		return int(math.Ceil(float64(results) / 20))
	}
	return 0
}

func extractor(soup string) Property {
	var property Property

	// Extract property features
	re := regexp.MustCompile(`<div class="property-features">.*?</div>`)
	propDiv := re.FindString(soup)
	if propDiv != "" {
		features := regexp.MustCompile(`<li.*?>(.*?)</li>`).FindAllString(propDiv, -1)
		for _, feature := range features {
			icon := regexp.MustCompile(`xlink:href="(.*?)"`).FindStringSubmatch(feature)
			value := regexp.MustCompile(`<span class="property-features__value">(.*?)</span>`).FindStringSubmatch(feature)
			if len(icon) > 1 && len(value) > 1 {
				switch {
				case strings.Contains(icon[1], "#listing-alt"):
					property.ListingID = strings.TrimSpace(value[1])
				case strings.Contains(icon[1], "#property-type"):
					property.PropertyType = strings.TrimSpace(value[1])
				case strings.Contains(icon[1], "#erf-size"):
					property.ErfSize = strings.TrimSpace(value[1])
				case strings.Contains(icon[1], "#property-size"):
					property.FloorSize = strings.TrimSpace(value[1])
				case strings.Contains(icon[1], "#rates"):
					property.RatesAndTaxes = strings.TrimSpace(value[1])
				case strings.Contains(icon[1], "#levies"):
					property.Levies = strings.TrimSpace(value[1])
				}
			}
		}
	}

	// Extract agent information
	reAgent := regexp.MustCompile(`const serverVariables\s*=\s*({.*?});`)
	agentMatch := reAgent.FindStringSubmatch(soup)
	if len(agentMatch) > 1 {
		var jsonData map[string]interface{}
		json.Unmarshal([]byte(agentMatch[1]), &jsonData)
		agencyInfo := jsonData["bundleParams"].(map[string]interface{})["agencyInfo"].(map[string]interface{})
		property.AgentName = agencyInfo["agencyName"].(string)
		property.AgentURL = "https://www.privateproperty.co.za" + agencyInfo["agencyPageUrl"].(string)
	}

	return property
}

func getIds(soup string) string {
	re := regexp.MustCompile(`"url":"(.*?)"`)
	match := re.FindStringSubmatch(soup)
	if len(match) > 1 {
		url := match[1]
		reID := regexp.MustCompile(`/([^/]+)$`)
		idMatch := reID.FindStringSubmatch(url)
		if len(idMatch) > 1 {
			return idMatch[1]
		}
	}
	return ""
}

func main() {
	lastPage := 3
	startPage := 1
	baseURL := "https://www.privateproperty.co.za/for-sale/mpumalanga/middelburg/1347"

	c := colly.NewCollector()

	var ids []string

	for p := startPage; p <= lastPage; p++ {
		pageURL := fmt.Sprintf("%s?page=%d", baseURL, p)
		c.OnHTML("a.listing-result", func(e *colly.HTMLElement) {
			ids = append(ids, getIds(e.ChildText("script")))
		})
		c.Visit(pageURL)
	}

	for _, listID := range ids {
		listURL := fmt.Sprintf("https://www.privateproperty.co.za/for-sale/something/something/something/%s", listID)
		c.OnHTML("html", func(e *colly.HTMLElement) {
			property := extractor(e.DOM.Html())
			fmt.Println(property)
		})
		c.Visit(listURL)
	}
}
