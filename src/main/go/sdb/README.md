
Golang Weather Data Generator

======================

  

Overview

--------

  

If you're reading this, I'm assuming that you are generally familiar with the 1 Billion Rows Challenge and have likely already cloned this repo locally.  If not, please review the readme in the root folder for much needed context.

My particular program is designed generate test data for the 1 Billion Rows Challenge using Go and only Go. Please note that this **NOT** a solution to the Challenge, but merely a tool to generate a large amount of test data. 

This program originated as a necessity; when I started looking at this challenge, I found that I had no easy way to generate 1 Billion rows of test data. The default instructions for data generation require Java, which I did not feel like installing just for this project, and while there are a couple of solutions in Go merged into this repo, there was no tool simply for creating test data using Go. 

This program generates simulated weather data for a predefined list of weather stations. It creates a file (`measurements.txt`) containing measurements for each station, including the station name and a randomly generated temperature value, conforming to the format of the 1 Billion Row Challenge:

    Sokyriany;66.8
    Araranguá;-63.2
    New Ulm;90.2

This program is designed to be flexible; you can specify however many rows you want to generate, although performance *will be affected for very large numbers of rows.*

Features

--------

  

- Customizable Data Generation: Users can specify the exact number of data rows they wish to generate.

- Simulated Weather Data: For each row, the program selects a weather station from a predefined list and assigns it a randomly generated temperature.

- Testability: The design incorporates dependency injection and interfaces, enhancing testability and maintainability.

  

Prerequisites

-------------

  

- Go (version 1.15 or newer recommended)

  

Ensure Go is installed and properly configured on your system. You can verify this by running `go version` in your terminal.


Usage

-----

  

To run the program, navigate to the project directory and use the following command:

  


  

`go run . [numRows]`

  

Replace `[numRows]` with the number of data rows you want to generate. For example:

  


  

`go run . 10000`

  

This command generates a file with 10,000 rows of simulated weather data.

This utility is **NOT** yet optimized, and as such  it currently takes 10-12 minutes to write a file with a full billion rows. As time permits, I'll try to come back and optimize, but at present, I consider this an acceptable tradeoff for a utility you'll probably run only once. 

  

Configuration

-------------

  

- Data File Location: The output file is saved to `/data` as `measurements.txt`.   Customize this path in the source code if necessary.

- Weather Station List: The list of weather stations is read from the `weather_stations.csv` file in `/data`. This file is included with the repo; please make sure it has not been messed with. 

  

Testing

-------

  

The program includes unit tests for its core functionality, although test coverage is not complete. Run these tests to ensure the program operates as expected:

  

`go test`
