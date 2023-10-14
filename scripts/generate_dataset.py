import argparse
import pandas as pd
import time
from countries import country


def store_dataset(dataset: pd.DataFrame, path: str) -> None:
    """
    Store dataset in csv file
    :param dataset: Pandas DataFrame
    :param path: path to store csv file
    :return: None
    """

    dataset.to_csv(path, index=False)


def generate_dataset(year_range: list) -> pd.DataFrame:
    """
    Generate dataset from html http://vitibrasil.cnpuv.embrapa.br/ sheet: Exportações
    :param year_range: list of years to extract data
    :return: Pandas DataFrame
    """
    dataset = pd.DataFrame(columns=["Country", "Quantity", "Value (US$)", "Year"])
    for year in year_range:
        print(f"Extracting data from {year}")
        url = f"http://vitibrasil.cnpuv.embrapa.br/index.php?ano={year}&opcao=opt_06&subopcao=subopt_01"
        try:
            df = pd.read_html(url, attrs={"class": "tb_base tb_dados"}, flavor=None, encoding="utf-8")[0]
        except ConnectionError:
            print(f"ConnectionError: {url}")
            print("Trying again...")
            time.sleep(20)
            df = pd.read_html(url, attrs={"class": "tb_base tb_dados"}, flavor=None, encoding="utf-8")[0]

        df.replace({"Países": country}, inplace=True)
        df.drop(df[df["Países"] == "Total"].index, inplace=True)

        df.rename({"Países": "Country",
                   "Quantidade (Kg)": "Quantity",
                   "Valor (US$)": "Value (US$)"},
                  axis=1,
                  inplace=True)
        df["Year"] = year

        print(f"Adding data from {year} to dataset")
        dataset = pd.concat([dataset, df], ignore_index=True)

    print(f"Extraction of data from {min(year_range)}-{max(year_range)} Done!")
    return dataset


def main():
    """
    Main function
    :return: None
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_year', help='year start to extract data', type=str, required=True)
    parser.add_argument('--end_year', help='year end to extract data', type=str)
    args = parser.parse_args()

    if args.end_year is None:
        args.end_year = args.start_year

    year_range = [year for year in range(int(args.start_year), int(args.end_year) + 1)]

    dataset = generate_dataset(year_range)
    print(f"Storing dataset in ../data/exportacao_{min(year_range)}_{max(year_range)}.csv")
    store_dataset(dataset, f"../data/exportacao_{min(year_range)}_{max(year_range)}.csv")
    print("Done!")


if __name__ == '__main__':
    main()
