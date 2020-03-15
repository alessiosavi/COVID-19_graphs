package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/client"
	"github.com/metakeule/fmtdate"
)

type WorldWideData struct {
	Date        time.Time
	State       string
	NewCases    int64
	NewDeaths   int64
	TotalCases  int64
	TotalDeaths int64
}

type ProvinceJsonData struct {
	Data                   string  `json:"data"`
	Stato                  string  `json:"stato"`
	CodiceRegione          int     `json:"codice_regione"`
	DenominazioneRegione   string  `json:"denominazione_regione"`
	CodiceProvincia        int     `json:"codice_provincia"`
	DenominazioneProvincia string  `json:"denominazione_provincia"`
	SiglaProvincia         string  `json:"sigla_provincia"`
	Lat                    float64 `json:"lat"`
	Long                   float64 `json:"long"`
	TotaleCasi             int     `json:"totale_casi"`
	Datetime               time.Time
}

type RegionsJsonData struct {
	Data                      string  `json:"data"`
	Stato                     string  `json:"stato"`
	CodiceRegione             int     `json:"codice_regione"`
	DenominazioneRegione      string  `json:"denominazione_regione"`
	Lat                       float64 `json:"lat"`
	Long                      float64 `json:"long"`
	RicoveratiConSintomi      int     `json:"ricoverati_con_sintomi"`
	TerapiaIntensiva          int     `json:"terapia_intensiva"`
	TotaleOspedalizzati       int     `json:"totale_ospedalizzati"`
	IsolamentoDomiciliare     int     `json:"isolamento_domiciliare"`
	TotaleAttualmentePositivi int     `json:"totale_attualmente_positivi"`
	NuoviAttualmentePositivi  int     `json:"nuovi_attualmente_positivi"`
	DimessiGuariti            int     `json:"dimessi_guariti"`
	Deceduti                  int     `json:"deceduti"`
	TotaleCasi                int     `json:"totale_casi"`
	Tamponi                   int     `json:"tamponi"`
	Datetime                  time.Time
}

const andamentoProvince string = "https://raw.githubusercontent.com/pcm-dpc/COVID-19/master/dati-json/dpc-covid19-ita-province.json"
const andamentoNazionale string = "https://raw.githubusercontent.com/pcm-dpc/COVID-19/master/dati-json/dpc-covid19-ita-andamento-nazionale.json"
const andamentoRegioni string = "https://raw.githubusercontent.com/pcm-dpc/COVID-19/master/dati-json/dpc-covid19-ita-regioni.json"
const andamentoMondiale string = "https://covid.ourworldindata.org/data/full_data.csv"
const DB_NAME string = "MyDB"
const HOSTNAME string = "http://localhost"

func main() {

	var (
		con          *client.Client // Client for push data into InfluxDB
		host         *url.URL       // Host related to the InfluxDB instance
		provinceData []ProvinceJsonData
		nationalData []RegionsJsonData
		regionData   []RegionsJsonData
		wordData     []WorldWideData
		err          error
	)

	initDatabase()

	// Initialize the URL for the InfluxDB instance
	if host, err = url.Parse(HOSTNAME + ":8086"); err != nil {
		panic(err)
	}
	// Initialize the InfluxDB client
	if con, err = client.NewClient(client.Config{URL: *host}); err != nil {
		panic(err)
	}
	// Verify that InfluxDB is available
	if _, _, err = con.Ping(); err != nil {
		panic(err)
	}

	provinceData = retrieveProvinceData(andamentoProvince)
	dbResponse := saveInfluxProvinceData(provinceData, con)
	fmt.Printf("%+v\n", dbResponse)
	provinceData = nil
	nationalData = retrieveNationalData(andamentoNazionale)
	dbResponse = saveInfluxNationalData(nationalData, con, "state_data")
	nationalData = nil
	fmt.Printf("%+v\n", dbResponse)
	regionData = retrieveNationalData(andamentoRegioni)
	dbResponse = saveInfluxNationalData(regionData, con, "regions_data")
	regionData = nil
	wordData = retrieveWorldWideData(andamentoMondiale)
	dbResponse = saveInfluxWordlData(wordData, con)
	fmt.Printf("%+v\n", dbResponse)
}

func initDatabase() {

	var (
		req       *http.Request
		resp      *http.Response
		err       error
		bodyBytes []byte
	)
	apiUrl := "http://localhost:8086"
	resource := "/query"
	data := url.Values{}
	data.Set("q", "CREATE DATABASE "+DB_NAME)

	u, _ := url.ParseRequestURI(apiUrl)
	u.Path = resource
	u.RawQuery = data.Encode()
	urlStr := fmt.Sprintf("%v", u)

	c := &http.Client{}
	if req, err = http.NewRequest("POST", urlStr, nil); err != nil {
		panic(err)
	}

	if resp, err = c.Do(req); err != nil {
		panic(err)
	}

	if resp.StatusCode != http.StatusOK {
		if bodyBytes, err = ioutil.ReadAll(resp.Body); err != nil {
			panic(err)
		}
		_ = resp.Body.Close()
		bodyString := string(bodyBytes)
		fmt.Printf("Error: %s\n", bodyString)
	} else {
		fmt.Printf("Database [%s] initialized\n", DB_NAME)
	}
}

func saveInfluxProvinceData(provinceData []ProvinceJsonData, con *client.Client) *client.Response {
	var dbResponse *client.Response // Response related to the data pushed into InfluxDB
	var err error

	// Initialize the list of event that have to be pushed into InfluxDB
	pts := make([]client.Point, len(provinceData))
	for i := range provinceData {
		fmt.Println("Case: ", provinceData[i])
		pts[i] = client.Point{
			Measurement: "all_province_data",
			Tags:        nil,
			Time:        provinceData[i].Datetime,
			Fields:      map[string]interface{}{provinceData[i].DenominazioneProvincia: provinceData[i].TotaleCasi}}
	}

	bps := client.BatchPoints{Points: pts, Database: DB_NAME}

	if dbResponse, err = con.Write(bps); err != nil {
		panic(err)
	}
	return dbResponse
}

func retrieveProvinceData(urlPath string) []ProvinceJsonData {
	var httpResponse *http.Response
	var err error
	var jsonData []ProvinceJsonData
	// Retrieve the fresh data related to covid-19
	if httpResponse, err = http.Get(urlPath); err != nil {
		panic(err)
	}
	defer httpResponse.Body.Close()

	buf := new(bytes.Buffer)
	buf.ReadFrom(httpResponse.Body)
	newBytes := buf.Bytes()
	trimmedBytes := bytes.Trim(newBytes, "\xef\xbb\xbf")
	//
	//decoder := json.NewDecoder(httpResponse.Body)
	//// Decode the json into the jsonData array
	//if err = decoder.Decode(&jsonData); err != nil {
	//	panic(err)
	//}
	json.Unmarshal(trimmedBytes, &jsonData)
	_ = httpResponse.Body.Close()

	var data []ProvinceJsonData
	for i := range jsonData {
		if jsonData[i].TotaleCasi > 0 {
			var t time.Time
			// Parse the time into a standard one
			if t, err = fmtdate.Parse("YYYY-MM-DD hh:mm:ss", jsonData[i].Data); err != nil {
				panic(err)
			}
			jsonData[i].Datetime = t
			data = append(data, jsonData[i])
		}
	}

	fmt.Printf("Retrieved %d data\n", len(jsonData))

	// Set the local time
	if loc, err := time.LoadLocation("Europe/Rome"); err != nil {
		panic(err)
	} else {
		time.Local = loc
	}

	return data
}

func retrieveNationalData(urlPath string) []RegionsJsonData {
	var httpResponse *http.Response
	var err error
	var jsonData []RegionsJsonData
	// Retrieve the fresh data related to covid-19
	if httpResponse, err = http.Get(urlPath); err != nil {
		panic(err)
	}
	defer httpResponse.Body.Close()

	//decoder := json.NewDecoder(httpResponse.Body)
	//// Decode the json into the jsonData array
	//if err = decoder.Decode(&jsonData); err != nil {
	//	panic(err)
	//}

	buf := new(bytes.Buffer)
	buf.ReadFrom(httpResponse.Body)
	newBytes := buf.Bytes()
	trimmedBytes := bytes.Trim(newBytes, "\xef\xbb\xbf")
	//
	//decoder := json.NewDecoder(httpResponse.Body)
	//// Decode the json into the jsonData array
	//if err = decoder.Decode(&jsonData); err != nil {
	//	panic(err)
	//}
	json.Unmarshal(trimmedBytes, &jsonData)
	_ = httpResponse.Body.Close()
	_ = httpResponse.Body.Close()

	var data []RegionsJsonData
	for i := range jsonData {
		var t time.Time
		// Parse the time into a standard one
		if t, err = fmtdate.Parse("YYYY-MM-DD hh:mm:ss", jsonData[i].Data); err != nil {
			panic(err)
		}
		jsonData[i].Datetime = t
		data = append(data, jsonData[i])

	}

	fmt.Printf("Retrieved %d data\n", len(jsonData))

	// Set the local time
	if loc, err := time.LoadLocation("Europe/Rome"); err != nil {
		panic(err)
	} else {
		time.Local = loc
	}

	return data
}

func saveInfluxNationalData(provinceData []RegionsJsonData, con *client.Client, dbName string) *client.Response {
	var dbResponse *client.Response // Response related to the data pushed into InfluxDB
	var err error

	// Initialize the list of event that have to be pushed into InfluxDB
	pts := make([]client.Point, len(provinceData))
	for i := range provinceData {
		fmt.Println("Case: ", provinceData[i])

		var m map[string]interface{} = make(map[string]interface{})
		m["codice_regione"] = provinceData[i].CodiceRegione
		m["denominazione_regione"] = provinceData[i].DenominazioneRegione
		m["lat"] = provinceData[i].Lat
		m["long"] = provinceData[i].Long
		m["ricoverati_con_sintomi"] = provinceData[i].RicoveratiConSintomi
		m["terapia_intensiva"] = provinceData[i].TerapiaIntensiva
		m["totale_ospedalizzati"] = provinceData[i].TotaleOspedalizzati
		m["isolamento_domiciliare"] = provinceData[i].IsolamentoDomiciliare
		m["totale_attualmente_positivi"] = provinceData[i].TotaleAttualmentePositivi
		m["nuovi_attualmente_positivi"] = provinceData[i].NuoviAttualmentePositivi
		m["dimessi_guariti"] = provinceData[i].DimessiGuariti
		m["deceduti"] = provinceData[i].Deceduti
		m["totale_casi"] = provinceData[i].TotaleCasi
		m["tamponi"] = provinceData[i].Tamponi

		pts[i] = client.Point{
			Measurement: dbName,
			Tags:        map[string]string{"regione": provinceData[i].DenominazioneRegione},
			Time:        provinceData[i].Datetime,
			Fields:      m}
	}

	bps := client.BatchPoints{Points: pts, Database: DB_NAME}

	if dbResponse, err = con.Write(bps); err != nil {
		panic(err)
	}
	return dbResponse
}

func retrieveWorldWideData(urlPath string) []WorldWideData {
	var (
		httpResponse *http.Response
		err          error
		lines        [][]string
		v            int64
	)

	if httpResponse, err = http.Get(urlPath); err != nil {
		panic(err)
	}
	defer httpResponse.Body.Close()
	if lines, err = csv.NewReader(httpResponse.Body).ReadAll(); err != nil {
		panic(err)
	}
	_ = httpResponse.Body.Close()

	var wordWideData []WorldWideData = make([]WorldWideData, len(lines))
	for i := 1; i < len(lines); i++ {
		var t time.Time
		// Parse the time into a standard one
		if t, err = fmtdate.Parse("YYYY-MM-DD", lines[i][0]); err != nil {
			panic(err)
		}
		wordWideData[i].Date = t
		wordWideData[i].State = lines[i][1]

		if lines[i][2] != "" {
			if v, err = strconv.ParseInt(lines[i][2], 10, 32); err != nil {
				panic(err)
			}
			wordWideData[i].NewCases = v
		}

		if lines[i][3] != "" {
			if v, err = strconv.ParseInt(lines[i][3], 10, 32); err != nil {
				panic(err)
			}
			wordWideData[i].NewDeaths = v
		}

		if lines[i][4] != "" {
			if v, err = strconv.ParseInt(lines[i][4], 10, 32); err != nil {
				panic(err)
			}
			wordWideData[i].TotalCases = v
		}

		if lines[i][5] != "" {
			if v, err = strconv.ParseInt(lines[i][5], 10, 32); err != nil {
				panic(err)
			}
			wordWideData[i].TotalDeaths = v
		}
	}
	return wordWideData
}

func saveInfluxWordlData(worldData []WorldWideData, con *client.Client) *client.Response {
	var dbResponse *client.Response // Response related to the data pushed into InfluxDB
	var err error

	// Initialize the list of event that have to be pushed into InfluxDB
	pts := make([]client.Point, len(worldData))
	for i := range worldData {
		fmt.Println("Case: ", worldData[i])

		var m map[string]interface{} = make(map[string]interface{})
		m["total_deaths"] = worldData[i].TotalDeaths
		m["total_cases"] = worldData[i].TotalCases
		m["new_deaths"] = worldData[i].NewDeaths
		m["new_cases"] = worldData[i].NewCases

		pts[i] = client.Point{
			Measurement: "all_world_data",
			Tags:        map[string]string{"nation": worldData[i].State},
			Time:        worldData[i].Date,
			Fields:      m}
	}
	bps := client.BatchPoints{Points: pts, Database: DB_NAME}

	if dbResponse, err = con.Write(bps); err != nil {
		panic(err)
	}
	return dbResponse
}

// The method is delegated to filter only the province that match the given input
func filterCasesForRegion(jsonData []ProvinceJsonData, regionName string) []ProvinceJsonData {
	var provinceData []ProvinceJsonData
	for i := range jsonData {
		if jsonData[i].DenominazioneRegione == regionName {
			provinceData = append(provinceData, jsonData[i])
		}
	}
	return provinceData
}
