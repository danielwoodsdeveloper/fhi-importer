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
	State              string             `xml:"State"`
	Scale              string             `xml:"Scale"`
	Corporate          Corporate          `xml:"Corporate"`
	ProductAmbulance   ProductAmbulance   `xml:"ProductAmbulance"`
	GeneralHealthCover GeneralHealthCover `xml:"GeneralHealthCover"`
	HospitalCover 	   HospitalCover      `xml:"HospitalCover"`
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

type HospitalCover struct {
	MedicalServices MedicalServices `xml:"MedicalServices"`
}

type MedicalServices struct {
	MedicalServices []MedicalService `xml:"MedicalService"`
}

type GeneralHealthService struct {
	Type    string `xml:"Title,attr"`
	Covered bool   `xml:"Covered,attr"`
}

type MedicalService struct {
	Type  string `xml:"Title,attr"`
	Cover string `xml:"Cover,attr"`
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

	// Filter out invalid products
	n := 0
	for _, product := range hospitalProducts.Products {
		if !product.Corporate.IsCorporate && product.IsValidScale() {
			hospitalProducts.Products[n] = product
			n++
		}
	}
	hospitalProducts.Products = hospitalProducts.Products[:n]

	n = 0
	for _, product := range extrasProducts.Products {
		if !product.Corporate.IsCorporate && product.IsValidScale() {
			extrasProducts.Products[n] = product
			n++
		}
	}
	extrasProducts.Products = extrasProducts.Products[:n]

	n = 0
	for _, product := range combinedProducts.Products {
		if !product.Corporate.IsCorporate && product.IsValidScale() {
			combinedProducts.Products[n] = product
			n++
		}
	}
	combinedProducts.Products = combinedProducts.Products[:n]

	// Reduce collection of policies by grouping by Name + FundCode + ProductGroupCode
	var reducedHospitalProducts Products
	var hospitalProductsMap map[string]bool = make(map[string]bool)
	for _, hospitalProduct := range hospitalProducts.Products {
		key := hospitalProduct.FundCode + "_" + hospitalProduct.ProductGroupCode + "_" + hospitalProduct.Name
		_, exists := hospitalProductsMap[key]
		if !exists {
			hospitalProductsMap[key] = true
			reducedHospitalProducts.Products = append(reducedHospitalProducts.Products, hospitalProduct)
		}
	}
	var reducedExtrasProducts Products
	var extrasProductsMap map[string]bool = make(map[string]bool)
	for _, extrasProduct := range extrasProducts.Products {
		key := extrasProduct.FundCode + "_" + extrasProduct.ProductGroupCode + "_" + extrasProduct.Name
		_, exists := extrasProductsMap[key]
		if !exists {
			extrasProductsMap[key] = true
			reducedExtrasProducts.Products = append(reducedExtrasProducts.Products, extrasProduct)
		}
	}
	var reducedCombinedProducts Products
	var combinedProductsMap map[string]bool = make(map[string]bool)
	for _, combinedProduct := range combinedProducts.Products {
		key := combinedProduct.FundCode + "_" + combinedProduct.ProductGroupCode + "_" + combinedProduct.Name
		_, exists := combinedProductsMap[key]
		if !exists {
			combinedProductsMap[key] = true
			reducedCombinedProducts.Products = append(reducedCombinedProducts.Products, combinedProduct)
		}
	}

	// Build custom combinations (warning, not optimised)
	var customCombinedProducts Products
	for _, hospitalProduct := range hospitalProducts.Products {
		for _, extrasProduct := range extrasProducts.Products {
			if extrasProduct.IsAmbulanceOnly() {
				continue
			}
			if !IsSameState(hospitalProduct, extrasProduct) {
				continue
			}
			if hospitalProduct.Scale != extrasProduct.Scale {
				continue
			}
			if extrasProduct.FundItemID == hospitalProduct.FundItemID {
				var product Product
				product.FundCode = hospitalProduct.FundCode + "+" + extrasProduct.FundCode
				product.ProductGroupCode = hospitalProduct.ProductGroupCode + "+" + extrasProduct.ProductGroupCode
				product.Name = hospitalProduct.Name + "+" + extrasProduct.Name
				customCombinedProducts.Products = append(customCombinedProducts.Products, product)
			}
		}
	}

	var reducedCustomCombinedProducts Products
	var customCombinedProductsMap map[string]bool = make(map[string]bool)
	for _, combinedProduct := range customCombinedProducts.Products {
		key := combinedProduct.FundCode + "_" + combinedProduct.ProductGroupCode + "_" + combinedProduct.Name
		_, exists := customCombinedProductsMap[key]
		if !exists {
			customCombinedProductsMap[key] = true
			reducedCustomCombinedProducts.Products = append(reducedCustomCombinedProducts.Products, combinedProduct)
		}
	}

	// Determine which extras policies are ambulance only
	var ambulanceExtrasProducts Products
	for _, extrasProduct := range reducedExtrasProducts.Products {
		if extrasProduct.IsAmbulanceOnly() {
			ambulanceExtrasProducts.Products = append(ambulanceExtrasProducts.Products, extrasProduct)
		}
	}

	// Determine which hospital policies are ambulance only
	var ambulanceHospitalProducts Products
	for _, hospitalProduct := range reducedHospitalProducts.Products {
		if hospitalProduct.IsAmbulanceOnly() {
			ambulanceHospitalProducts.Products = append(ambulanceHospitalProducts.Products, hospitalProduct)
		}
	}

	// Determine which combined policies are ambulance only
	var ambulanceCombinedProducts Products
	for _, combinedProduct := range reducedCombinedProducts.Products {
		if combinedProduct.IsAmbulanceOnly() {
			ambulanceCombinedProducts.Products = append(ambulanceCombinedProducts.Products, combinedProduct)
		}
	}

	fmt.Println("Hospital Products: " + strconv.Itoa(len(reducedHospitalProducts.Products)))
	fmt.Println("Extras Products: " + strconv.Itoa(len(reducedExtrasProducts.Products)))
	fmt.Println("Combined Products: " + strconv.Itoa(len(reducedCombinedProducts.Products)))
	fmt.Println("Hospital Variants: " + strconv.Itoa(len(hospitalProducts.Products)))
	fmt.Println("Extras Variants: " + strconv.Itoa(len(extrasProducts.Products)))
	fmt.Println("Combined Variants: " + strconv.Itoa(len(combinedProducts.Products)))

	fmt.Println("Custom Combined Products: " + strconv.Itoa(len(customCombinedProducts.Products)))
	fmt.Println("Custom Combined Variants: " + strconv.Itoa(len(reducedCustomCombinedProducts.Products)))

	fmt.Println("Ambulance Only (Hospital): " + strconv.Itoa(len(ambulanceHospitalProducts.Products)))
	fmt.Println("Ambulance Only (Extras): " + strconv.Itoa(len(ambulanceExtrasProducts.Products)))
	fmt.Println("Ambulance Only (Combined): " + strconv.Itoa(len(ambulanceCombinedProducts.Products)))

	// // Insert into DB
	// fundsStmt, err := db.Prepare("INSERT INTO funds(FundItemID, FundID, Status, FundCode, FundName, FundType) VALUES (?, ?, ?, ?, ?, ?)")
	// if err != nil {
	// 	panic(err)
	// }

	// productsStmt, err := db.Prepare("INSERT INTO products(ProductItemID, ProductID, ProductCode, FundItemID, Status, FundCode, ProductGroupCode, Name, ProductType, FundsProductCode, ProductStatus) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	// if err != nil {
	// 	panic(err)
	// }

	// for _, fund := range funds.Funds {
	// 	_, err = fundsStmt.Exec(fund.FundItemID, fund.FundID, fund.Status, fund.FundCode, fund.FundName, fund.FundType)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }

	// for _, product := range hospitalProducts.Products {
	// 	_, err = productsStmt.Exec(product.ProductItemID, product.ProductID, product.ProductCode, product.FundItemID, product.Status, product.FundCode, product.ProductGroupCode, product.Name, product.ProductType, product.FundsProductCode, product.ProductStatus)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }

	// for _, product := range extrasProducts.Products {
	// 	_, err = productsStmt.Exec(product.ProductItemID, product.ProductID, product.ProductCode, product.FundItemID, product.Status, product.FundCode, product.ProductGroupCode, product.Name, product.ProductType, product.FundsProductCode, product.ProductStatus)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }

	// for _, product := range combinedProducts.Products {
	// 	_, err = productsStmt.Exec(product.ProductItemID, product.ProductID, product.ProductCode, product.FundItemID, product.Status, product.FundCode, product.ProductGroupCode, product.Name, product.ProductType, product.FundsProductCode, product.ProductStatus)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }

	// for _, product := range customCombinedProducts.Products {
	// 	_, err = productsStmt.Exec(product.ProductItemID, product.ProductID, product.ProductCode, product.FundItemID, product.Status, product.FundCode, product.ProductGroupCode, product.Name, product.ProductType, product.FundsProductCode, product.ProductStatus)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }
}

func IsSameState(a Product, b Product) bool {
	if a.State == "ALL" || b.State == "ALL" {
		return true
	}

	return a.State == b.State
}

func (product Product) IsAmbulanceOnly() bool {
	// If no ambulance coverage, not ambulance only
	if product.ProductAmbulance.Ambulance.Cover == "None" {
		return false
	}

	// If has a health service, not ambulance only
	for _, service := range product.GeneralHealthCover.GeneralHealthServices.GeneralHealthServices {
		if service.Covered {
			return false
		}
	}

	// If has a medical service, not ambulance only
	for _, service := range product.HospitalCover.MedicalServices.MedicalServices {
		if service.Cover != "NotCovered" {
			return false
		}
	}

	return true
}

func (product Product) IsValidScale() bool {
	return product.Scale != "ChildrenOnly" && product.Scale != "ExtendedFamily" && product.Scale != "SingleAnyDependants" && product.Scale != "CoupleAnyDependants"
}
