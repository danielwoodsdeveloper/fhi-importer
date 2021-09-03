package main

import (
	"database/sql"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Product XML types
type Products struct {
	Products []Product `xml:"Product"`
}

type Product struct {
	ProductItemID      string             `xml:"ProductItemID,attr"`
	ProductID          string             `xml:"ProductID,attr"`
	ProductCode        string             `xml:"ProductCode,attr"`
	FundItemID         string             `xml:"FundItemID,attr"`
	Status             string             `xml:"Status,attr"`
	FundCode           string             `xml:"FundCode"`
	ProductGroupCode   string             `xml:"ProductGroupCode"`
	Name               string             `xml:"Name"`
	ProductType        string             `xml:"ProductType"`
	FundsProductCode   string             `xml:"FundsProductCode"`
	ProductStatus      string             `xml:"ProductStatus"`
	Corporate          Corporate          `xml:"Corporate"`
	ProductAmbulance   ProductAmbulance   `xml:"ProductAmbulance"`
	GeneralHealthCover GeneralHealthCover `xml:"GeneralHealthCover"`
}

type Corporate struct {
	IsCorporate bool `xml:"IsCorporate,attr"`
}

type ProductAmbulance struct {
	Ambulance Ambulance `xml:"Ambulance"`
}

type Ambulance struct {
	Cover string `xml:"Cover,attr"`
}

type GeneralHealthCover struct {
	GeneralHealthServices GeneralHealthServices `xml:"GeneralHealthServices"`
}

type GeneralHealthServices struct {
	GeneralHealthServices []GeneralHealthService `xml:"GeneralHealthService"`
}

type GeneralHealthService struct {
	Type    string `xml:"Title"`
	Covered bool   `xml:"Covered"`
}

// Fund XML types
type Funds struct {
	Funds []Fund `xml:"Fund"`
}

type Fund struct {
	FundItemID string `xml:"FundItemID,attr"`
	FundID     string `xml:"FundID,attr"`
	Status     string `xml:"Status,attr"`
	// StatusDate   string `xml:"StatusDate,attr"`
	// DateModified string `xml:"DateModified,attr"`
	// DateCreated  string `xml:"DateCreated,attr"`
	FundCode string `xml:"FundCode"`
	FundName string `xml:"FundName"`
	FundType string `xml:"FundType"`
}

func main() {
	// Set-up DB
	db, err := sql.Open("mysql", "root:abcd1234@/scratch")
	if err != nil {
		panic(err)
	}
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	defer db.Close()

	_, err = db.Query("DROP TABLE IF EXISTS products")
	if err != nil {
		panic(err)
	}

	_, err = db.Query(`CREATE TABLE IF NOT EXISTS products(
        id int primary key auto_increment,
        ProductItemID text,
        ProductID text,
        ProductCode text,
        FundItemID text,
        Status text,
        FundCode text,
        ProductGroupCode text,
        Name text,
        ProductType text,
        FundsProductCode text,
        ProductStatus text)`)
	if err != nil {
		panic(err)
	}

	_, err = db.Query("DROP TABLE IF EXISTS funds")
	if err != nil {
		panic(err)
	}

	_, err = db.Query(`CREATE TABLE IF NOT EXISTS funds(
        id int primary key auto_increment,
        FundItemID text,
        FundID text,
        Status text,
        FundCode text,
        FundName text,
        FundType text)`)
	if err != nil {
		panic(err)
	}

	// Funds
	fundsFile, err := os.Open("funds.xml")
	if err != nil {
		panic(err)
	}

	defer fundsFile.Close()

	fundsBytes, _ := ioutil.ReadAll(fundsFile)
	var funds Funds
	xml.Unmarshal(fundsBytes, &funds)

	// Hospital
	hospitalFile, err := os.Open("hospital.xml")
	if err != nil {
		panic(err)
	}

	defer fundsFile.Close()

	hospitalBytes, _ := ioutil.ReadAll(hospitalFile)
	var hospitalProducts Products
	xml.Unmarshal(hospitalBytes, &hospitalProducts)

	// Extras
	extrasFile, err := os.Open("extras.xml")
	if err != nil {
		panic(err)
	}

	defer fundsFile.Close()

	extrasBytes, _ := ioutil.ReadAll(extrasFile)
	var extrasProducts Products
	xml.Unmarshal(extrasBytes, &extrasProducts)

	// Combined
	combinedFile, err := os.Open("combined.xml")
	if err != nil {
		panic(err)
	}

	defer fundsFile.Close()

	combinedBytes, _ := ioutil.ReadAll(combinedFile)
	var combinedProducts Products
	xml.Unmarshal(combinedBytes, &combinedProducts)

	// Filter out corporate products
	n := 0
	for _, product := range hospitalProducts.Products {
		if !product.Corporate.IsCorporate {
			hospitalProducts.Products[n] = product
			n++
		}
	}
	hospitalProducts.Products = hospitalProducts.Products[:n]

	n = 0
	for _, product := range extrasProducts.Products {
		if !product.Corporate.IsCorporate {
			extrasProducts.Products[n] = product
			n++
		}
	}
	extrasProducts.Products = extrasProducts.Products[:n]

	n = 0
	for _, product := range combinedProducts.Products {
		if !product.Corporate.IsCorporate {
			combinedProducts.Products[n] = product
			n++
		}
	}
	combinedProducts.Products = combinedProducts.Products[:n]

	// Build custom combinations (warning, not optimised)
	var customCombinedProducts Products
	for _, hospitalProduct := range hospitalProducts.Products {
		for _, extrasProduct := range extrasProducts.Products {
			if extrasProduct.IsAmbulanceOnly() {
				continue
			}
			if extrasProduct.FundItemID == hospitalProduct.FundItemID {
				var customCombinedProduct Product
				customCombinedProduct.Name = hospitalProduct.Name + " + " + extrasProduct.Name
				customCombinedProduct.ProductType = "CustomCombined"
				customCombinedProducts.Products = append(customCombinedProducts.Products, customCombinedProduct)
			}
		}
	}

	fmt.Println("Custom Combined: " + strconv.Itoa(len(customCombinedProducts.Products)))
	fmt.Println("Hospital: " + strconv.Itoa(len(hospitalProducts.Products)))
	fmt.Println("Extras: " + strconv.Itoa(len(extrasProducts.Products)))
	fmt.Println("Combined: " + strconv.Itoa(len(combinedProducts.Products)))

	// Insert into DB
	fundsStmt, err := db.Prepare("INSERT INTO funds(FundItemID, FundID, Status, FundCode, FundName, FundType) VALUES (?, ?, ?, ?, ?, ?)")
	if err != nil {
		panic(err)
	}

	productsStmt, err := db.Prepare("INSERT INTO products(ProductItemID, ProductID, ProductCode, FundItemID, Status, FundCode, ProductGroupCode, Name, ProductType, FundsProductCode, ProductStatus) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		panic(err)
	}

	for _, fund := range funds.Funds {
		_, err = fundsStmt.Exec(fund.FundItemID, fund.FundID, fund.Status, fund.FundCode, fund.FundName, fund.FundType)
		if err != nil {
			panic(err)
		}
	}

	for _, product := range hospitalProducts.Products {
		_, err = productsStmt.Exec(product.ProductItemID, product.ProductID, product.ProductCode, product.FundItemID, product.Status, product.FundCode, product.ProductGroupCode, product.Name, product.ProductType, product.FundsProductCode, product.ProductStatus)
		if err != nil {
			panic(err)
		}
	}

	for _, product := range extrasProducts.Products {
		_, err = productsStmt.Exec(product.ProductItemID, product.ProductID, product.ProductCode, product.FundItemID, product.Status, product.FundCode, product.ProductGroupCode, product.Name, product.ProductType, product.FundsProductCode, product.ProductStatus)
		if err != nil {
			panic(err)
		}
	}

	for _, product := range combinedProducts.Products {
		_, err = productsStmt.Exec(product.ProductItemID, product.ProductID, product.ProductCode, product.FundItemID, product.Status, product.FundCode, product.ProductGroupCode, product.Name, product.ProductType, product.FundsProductCode, product.ProductStatus)
		if err != nil {
			panic(err)
		}
	}

	// for _, product := range customCombinedProducts.Products {
	// 	_, err = productsStmt.Exec(product.ProductItemID, product.ProductID, product.ProductCode, product.FundItemID, product.Status, product.FundCode, product.ProductGroupCode, product.Name, product.ProductType, product.FundsProductCode, product.ProductStatus)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }
}

func (product Product) IsAmbulanceOnly() bool {
	// If no ambulance coverage, not ambulance only
	if product.ProductAmbulance.Ambulance.Cover != "None" {
		return false
	}

	// If has a health service, not ambulance only
	for _, service := range product.GeneralHealthCover.GeneralHealthServices.GeneralHealthServices {
		if service.Covered {
			return false
		}
	}

	return true
}
