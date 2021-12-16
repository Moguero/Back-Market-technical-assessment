# Back Market technical assessment : collect-prepare challenge 🔎

## Context

This challenge aims at separating the valid rows from the invalid ones in a CSV file, resulting in two separate files in Parquet format. <br>
**Use case** : the business team wants only the products with an image, but wants to archive the products without image. <br>
The full wording can be found in this [GitHub repository](https://github.com/BackMarket/jobs/tree/master/data_prepare_team). <br><br>

I decided to handle this problem in two parts :

- a first exploratory part where I was working with an interactive Jupyter notebook, which is more handy for plotting and fast experimenting
- a second part focusing on the production code, where I wrote a proper Python script

Now let's start 🚀

## Getting started

Firstly, clone this GitHub repository on your machine. Navigate to the cloned repository and create the project environment from the given *environment.yml* file.
For example, using the [Anaconda](https://docs.anaconda.com/anaconda/install/) package manager :

```
conda env create -f environment.yml
```

Then, activate the virtual environment. For example, with conda :

```
conda activate back-market-case-study-lin
```

*Warning* :  The conda environment was created on a **Linux** machine, the previous commands weren't tested on Windows nor MacOS.

<br>

*Note* : Once you're done with the project, don't forget to remove the environment by running :

```
conda env remove -n back-market-case-study-lin
```

## Usage

### Notebook

First, run the following command in your terminal to add the conda environment in the Jupyter notebook :

```
 python -m ipykernel install --user --name=back-market-case-study-lin
```

Open the notebook in your IDE and choose the kernel associated to the environment named *back-market-case-study-lin*.

You can now execute the notebook. You will find in it an incorporated report where I explain everything about my first approach of the problem.

<br>

### CLI interface

In order to execute the transformer Python program in a terminal, navigate to the root of the GitHub repository and type this CLI command :

```
python transformer/transform.py ./resources/product_catalog.csv ./valid_product_catalog.parquet ./invalid_product_catalog.parquet
```
<br>

*Note* : the execution will fail if one of the output file paths already exists. To re-run the script, delete the old parquet files first.

To get more help about the parser, type :

```
python transformer/transform.py -h
```

In particular, you can specify the library with which you'd like to handle the CSV, thanks to the "--library" optional argument : the value can be either pandas or dask.

--- 

I wish you a good reading 🙂
