# this script ONLY for extracting coordinates from given poi original excel file

import requests
import re
from urllib import parse
from time import sleep
from random import randint

import polars as pl
from playwright.sync_api import sync_playwright


def get_coordinates(query: str) -> str:
    # Warning: google maps may is blocking the request if it is too frequent

    # create url with query encoded
    url = "https://www.google.com/maps/search/" + parse.quote(query)

    response = requests.get(url)
    # print(response.text)
    # save response to file
    with open("response.html", "w") as f:
        f.write(response.text)

    # extract with regex from response, ex: "/@21.0304733,105.8431508" -> 21.0304733,105.8431508
    pattern = re.compile(r"@\d{2}\.\d+,\d{3}\.\d+")
    match = pattern.search(response.text)

    assert match is not None
    assert (
        len(match.groups()) == 1
    ), "the query should only return 1 match because it is an address"

    return match.group()


def get_coordinates_playwright(query: str) -> str:

    url = "https://www.google.com/maps/search/" + parse.quote(query)

    new_url = None
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        sleep(randint(1, 5))
        page = browser.new_page()
        page.goto(url)
        initial_url = page.url

        page.wait_for_function("window.location.href !== '{}'".format(initial_url))

        new_url = page.url

        browser.close()

    # extract with regex from response, ex: "/@21.0304733,105.8431508"
    pattern = re.compile(r"@\d{2}\.\d+,\d{3}\.\d+")
    match = pattern.search(new_url)

    assert match is not None
    assert (
        len(match.groups()) == 0
    ), "the query should only return 1 match because it is an address"

    return match.group().replace("@", "")


def get_search_link(query: str) -> str:

    url = "https://www.google.com/maps/search/" + parse.quote(query)

    new_url = None
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        # sleep to avoid being blocked
        sleep(randint(1, 5))
        page = browser.new_page()
        page.goto(url)
        initial_url = page.url

        page.wait_for_function("window.location.href !== '{}'".format(initial_url))

        new_url = page.url

        browser.close()

    assert new_url != url, "the url should have changed"

    return new_url


def main():
    df = pl.read_excel("./datasets/poi_original.xlsx")
    # add a column with the links using the address column
    df = df.with_columns(
        pl.col("Address Line 1")
        .map_elements(get_search_link, return_dtype=pl.String)
        .alias("link")
    )
    # save to file
    df.write_parquet("poi_with_coordinates.parquet")


if __name__ == "__main__":
    main()
