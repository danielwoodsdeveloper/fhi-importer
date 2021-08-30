package main

import (
	"database/sql"
	"encoding/xml"
	"io/ioutil"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Products struct {
	Products []Product `xml:"Product"`
}

type Product struct {
	ProductItemID    string `xml:"ProductItemID,attr"`
	ProductID        string `xml:"ProductID,attr"`
	ProductCode      string `xml:"ProductCode,attr"`
	FundItemID       string `xml:"FundItemID,attr"`
	Status           string `xml:"Status,attr"`
	FundCode         string `xml:"FundCode"`
	ProductGroupCode string `xml:"ProductGroupCode"`
	Name             string `xml:"Name"`
	ProductType      string `xml:"ProductType"`
	FundsProductCode string `xml:"FundsProductCode"`
	ProductStatus    string `xml:"ProductStatus"`
}

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

	// Insert
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
}
