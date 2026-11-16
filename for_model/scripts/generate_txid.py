import sys
import bs4
import requests
from x_client_transaction.utils import generate_headers, get_ondemand_file_url
from x_client_transaction import ClientTransaction


def main():
    if len(sys.argv) != 3:
        print("usage: generate_txid.py <method> <path>", file=sys.stderr)
        sys.exit(1)

    method = sys.argv[1]
    path = sys.argv[2]

    try:
        session = requests.Session()
        session.headers = generate_headers()

        home_page = session.get(url="https://x.com/home", timeout=15)
        home_page.raise_for_status()
        home_page_response = bs4.BeautifulSoup(home_page.content, "html.parser")

        ondemand_file_url = get_ondemand_file_url(response=home_page_response)
        ondemand_file = session.get(url=ondemand_file_url, timeout=15)
        ondemand_file.raise_for_status()
        ondemand_file_response = bs4.BeautifulSoup(ondemand_file.content, "html.parser")

        ct = ClientTransaction(
            home_page_response=home_page_response,
            ondemand_file_response=ondemand_file_response,
        )
        transaction_id = ct.generate_transaction_id(method=method, path=path)

        print(transaction_id)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
