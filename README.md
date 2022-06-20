# RDrive Alpha

RDrive is a blockchain storage solution that allows you to interact with blockchain in a seamless manner.

# How to use RDrive

Download the release of RDrive from github releases and extract the archive.
Once extracted, open terminal and use the following command to install RDrive:

$ ./rdrive --run

Here are the arguments available:

--run                           Run RDrive
--contractName <string>         Name of the contract
--boxName <string>              Desired box name
--masterRegUri <string>         rchain-token master registry uri (leave blank to create a new one)
--privKey <string>              Private key for your wallet
--readOnlyHost <url> <url> ...  List of observer nodes
--validatorHost <url> <url> ... List of validator nodes
--mnt <path>                    ABsolute path for where to mount RDrive
--shardId <string>              Shard name
--pursePrice <number>           Default price for NFTs, will be set in /.token.conf


For example: 
$ ./rdrive --run --contractName mynft --boxName theo --masterRegUri 9jo81oy9bptprsox9asqr377q3msjjyrbtcwimxgrcpmszdxw185bx --privKey 6428f75c09db8b3a260fc1dcb1c93619bd3eecf6787b003ddc6ba5e87025c177 --readOnlyHost http://127.0.0.1:40403 --validatorHost http://127.0.0.1:40403 --mnt /home/theoxd/demo/rdrive --shardId root --pursePrice 500000000

# How to build

# TODO:
* Persistent caching
* Improve memory usage
* Fix reading of files larger than 40mb+ due to explore deploy failing because of cost accounting.