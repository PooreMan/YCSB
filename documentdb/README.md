## Quick Start

This section describes how to run YCSB on DocumentDB running locally. 

### 1. Start DocumentDB

Provision a DocumentDB database in the Microsoft Azure panel. (detailed instructions to come later)

### 2. Set Up YCSB

Clone the YCSB git repository and compile:

    git clone git://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn clean package

### 3. Run YCSB
    
Now you are ready to run! First, load the data:

    ./bin/ycsb load docdb -s -P workloads/workloada

Then, run the workload:

    ./bin/ycsb run docdb -s -P workloads/workloada

See the next section for the list of configuration parameters for DocumentDBDB.

## DocumentDB Configuration Parameters

### `docdb.endpoint` (default: ``)

### `docdb.masterkey` (default: ``)

### `docdb.database` (default: `ycsb`)

### `docdb.collection` (default `benchmarking`)
