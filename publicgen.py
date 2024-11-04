import aiohttp
import asyncio
import time
import csv

# Asynchronous function to handle API requests with headers and retries
async def fetch_balance(session, address, headers):
    url = f"https://blockchain.info/q/pubkeyaddr/{address}"
    retries = 2
    timeout = aiohttp.ClientTimeout(total=10)  # 10 seconds timeout per request
    for attempt in range(retries):
        try:
            async with session.get(url, headers=headers, timeout=timeout) as response:
                if response.status == 200:
                    data = await response.text()
                    return address, data
                else:
                    return address, None
        except Exception as e:
            if attempt == retries - 1:
                print(f"Failed to fetch {address} after {retries} attempts: {e}")
                return address, None
            await asyncio.sleep(2)  # Backoff before retry

# Function to process addresses in batches using async
async def process_addresses(addresses_file, output_file, batch_size=1000):
    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; BlockchainBot/1.0)',
        'X-Requested-With': 'XMLHttpRequest',
        'Cache-Control': 'no-cache'
    }

    # Open the output file to log results
    with open(output_file, 'a', encoding='utf-8') as result_file:
        # Create an asynchronous session for API requests
        async with aiohttp.ClientSession() as session:
            # Process addresses in batches
            batch = []
            async for address in stream_addresses(addresses_file):
                batch.append(address)
                if len(batch) >= batch_size:
                    results = await asyncio.gather(*[fetch_balance(session, addr, headers) for addr in batch])
                    for address, balance in results:
                        if balance:
                            result_file.write(f"{address},{balance}\n")
                    batch.clear()  # Clear the batch after processing

            # Process any remaining addresses
            if batch:
                results = await asyncio.gather(*[fetch_balance(session, addr, headers) for addr in batch])
                for address, balance in results:
                    if balance:
                        result_file.write(f"{balance}\n")

# Efficient generator to stream addresses from a file without loading the entire file into memory
async def stream_addresses(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            yield row[0].strip()  # Assuming each row contains one address

# Main function to start the event loop and process the addresses
def main():
    addresses_file = 'puzzadd.txt'
    output_file = 'results.txt'
    batch_size = 1000  # Adjust based on network and system capability

    print("\n\n########################################################")
    print("Processing addresses with SHUBSAINI08 UPDATES.")
    print("########################################################\n\n")

    start_time = time.time()

    # Run the asynchronous process
    asyncio.run(process_addresses(addresses_file, output_file, batch_size))

    print(f"\nProcessing completed in {time.time() - start_time:.2f} seconds")

# Start the process
if __name__ == "__main__":
    main()

