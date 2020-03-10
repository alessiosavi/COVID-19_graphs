package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"time"

	"github.com/influxdata/influxdb/client"
	"github.com/metakeule/fmtdate"
)

type JsonData struct {
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

func main() {

	filePath := "/home/alessiosavi/WORKSPACE/COVID-19/dati-json/dpc-covid19-ita-province.json"
	var j []byte
	var jsonData []JsonData
	var err error
	if j, err = ioutil.ReadFile(filePath); err != nil {
		panic(err)
	}

	if err = json.Unmarshal(j, &jsonData); err != nil {
		panic(err)
	}
	fmt.Printf("%d\n", len(jsonData))
	var touscanyData []JsonData

	if loc, err := time.LoadLocation("Europe/Rome"); err != nil {
		panic(err)
	} else {
		time.Local = loc
	}
	// Create a client
	//var graphiteClient *graphite.Client
	//if graphiteClient, err = graphite.NewClient("tcp://127.0.0.1:2003", "", 1*time.Second); err != nil {
	//	panic(err)
	//}

	host, err := url.Parse(fmt.Sprintf("http://%s:%d", "localhost", 8086))
	if err != nil {
		panic(err)
	}
	con, err := client.NewClient(client.Config{URL: *host})
	if err != nil {
		panic(err)
	}

	for i := range jsonData {
		if jsonData[i].DenominazioneRegione == "Toscana" {
			if jsonData[i].TotaleCasi > 0 {
				touscanyData = append(touscanyData, jsonData[i])
			}
		}
	}
	pts := make([]client.Point, len(touscanyData))

	for i := range touscanyData {
		var t time.Time

		t, err := fmtdate.Parse("YYYY-MM-DD hh:mm:ss", touscanyData[i].Data)
		if err != nil {
			panic(err)
		}

		touscanyData[i].Datetime = t
		fmt.Println(touscanyData[i].Datetime)

		fmt.Println("Case: ", touscanyData[i])
		pts[i] = client.Point{
			Measurement: "all_touscany_case",
			Tags:        nil,
			Time:        touscanyData[i].Datetime,
			Fields: map[string]interface{}{
				touscanyData[i].DenominazioneProvincia: touscanyData[i].TotaleCasi}}

		bps := client.BatchPoints{
			Points:   pts,
			Database: "mydb"}
		r, err := con.Write(bps)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%+v\n", r)
		//// Create a Metric instance, and send this metric
		//metric := &graphite.Metric{
		//	Name:      "totale_casi",
		//	Value:     touscanyData[i].TotaleCasi,
		//	Timestamp: touscanyData[i].Datetime,
		//}
		////fmt.Printf("Inserting: %+v", metric)
		//graphiteClient.SendMetric(metric)
	}
	// Shutdown will block the caller until all metrics in buffer have been sent, or timeout occurs.
	// If timeout occurs before all metrics flushed, metrics left in buffer will be lost.
	//graphiteClient.Shutdown(10 * time.Second)

}
