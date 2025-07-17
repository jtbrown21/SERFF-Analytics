from serff_analytics.ingest.airtable_sync import AirtableSync

syncer = AirtableSync()

print("Testing Airtable pagination...")

seen_ids = set()
duplicates = []
page_count = 0

for page in syncer.table.iterate(page_size=100):
    page_count += 1
    print(f"Page {page_count}: {len(page)} records")
    
    for record in page:
        record_id = record['id']
        if record_id in seen_ids:
            duplicates.append(record_id)
            print(f"  DUPLICATE on page {page_count}: {record_id}")
        else:
            seen_ids.add(record_id)
    
    # Stop after a few pages to see pattern
    if page_count >= 5:
        print(f"\nStopping early. Found {len(duplicates)} duplicates in first 5 pages")
        break

print(f"\nTotal unique: {len(seen_ids)}")
print(f"Total duplicates: {len(duplicates)}")