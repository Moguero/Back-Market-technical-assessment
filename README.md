# Back Market technical assessment : collect-prepare challenge 🔎

## Context

This challenge aims at separating the valid rows from the invalid ones in a CSV file, resulting in two separate files in Parquet format. <br>
**Use case** : the business team wants only the products with an image, but wants to archive the products without image. <br>
The full wording can be found in this [GitHub repository](https://github.com/BackMarket/jobs/tree/master/data_prepare_team). <br><br>

I decided to handle this problem in two parts :

- a first exploratory part where I was working with an interactive Jupyter notebook, which is more handy for plotting and fast experimenting
- a second part focusing on the production code, where I designed a basic CLI interface

## Getting started

Firstly, clone this GitHub repository on your machine. Navigate to the cloned repository and create the project environment from the given environment.yml file.
For example, using the [Anaconda](https://docs.anaconda.com/anaconda/install/) package manager :

```
conda env create -f environment.yml
```

Then, activate the virtual environment. For example, with conda :

```
conda activate back-market-case-study-lin
```

*Note* : Once you're done with the project, don't forget to remove the environment by running :

```
conda env remove -n back-market-case-study-lin
```

## Usage

### Notebook

Open the notebook in your IDE and choose the kernel associated to the Linux environment named *back-market-case-study-lin*.

You can now execute the notebook cells. You will find in it an incorporated report where I explain everthing about my first approach of the problem.

### CLI interface

In order to execute the transformer Python program, navigate to the root of the GitHub repository type this CLI command :

```
python transformer/transform.py "./resources/product_catalog.csv" "./valid_product_catalog.parquet" "./invalid_product_catalog.parquet"
```

*Note* : the execution will fail if one of the output file paths already exists. To re-run the script, delete the old parquet files first.

To get more help about the parser, type :

```
python transformer/transform.py -h
```
--- 

I wish you a good reading 🙂
