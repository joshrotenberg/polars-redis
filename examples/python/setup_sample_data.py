#!/usr/bin/env python3
"""
Setup sample data in Redis for the polars-redis examples.

This script creates sample user hashes that the examples can scan.

Run with: python setup_sample_data.py

Environment variables:
    REDIS_URL: Redis connection URL (default: redis://localhost:6379)
    NUM_USERS: Number of users to create (default: 100)
"""

import os
import random
import redis

# Sample data generators
FIRST_NAMES = [
    "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry",
    "Ivy", "Jack", "Kate", "Leo", "Maya", "Noah", "Olivia", "Paul",
    "Quinn", "Rose", "Sam", "Tara", "Uma", "Victor", "Wendy", "Xavier",
    "Yara", "Zack",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
]

DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "example.com", "test.org"]


def generate_user(user_id: int) -> dict:
    """Generate a random user record."""
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    domain = random.choice(DOMAINS)
    
    return {
        "name": f"{first} {last}",
        "email": f"{first.lower()}.{last.lower()}{user_id}@{domain}",
        "age": str(random.randint(18, 75)),
        "score": f"{random.uniform(0, 100):.2f}",
        "active": "true" if random.random() > 0.3 else "false",
        "signup_ts": str(random.randint(1609459200, 1735689600)),  # 2021-2025
    }


def main():
    url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    num_users = int(os.environ.get("NUM_USERS", "100"))
    
    print(f"Connecting to: {url}")
    print(f"Creating {num_users} sample users...")
    
    # Connect to Redis
    client = redis.from_url(url)
    
    # Test connection
    try:
        client.ping()
        print("Connected successfully!")
    except redis.ConnectionError as e:
        print(f"Failed to connect to Redis: {e}")
        print("Make sure Redis is running on the specified URL.")
        return 1
    
    # Clear existing sample data
    print("\nClearing existing user:* keys...")
    cursor = 0
    deleted = 0
    while True:
        cursor, keys = client.scan(cursor, match="user:*", count=1000)
        if keys:
            client.delete(*keys)
            deleted += len(keys)
        if cursor == 0:
            break
    print(f"Deleted {deleted} existing keys")
    
    # Create new users using pipeline for efficiency
    print(f"\nCreating {num_users} new users...")
    pipe = client.pipeline()
    
    for i in range(1, num_users + 1):
        user_data = generate_user(i)
        key = f"user:{i}"
        pipe.hset(key, mapping=user_data)
        
        # Execute in batches of 1000
        if i % 1000 == 0:
            pipe.execute()
            pipe = client.pipeline()
            print(f"  Created {i} users...")
    
    # Execute remaining
    pipe.execute()
    print(f"  Created {num_users} users")
    
    # Verify
    print("\nVerifying...")
    cursor = 0
    count = 0
    while True:
        cursor, keys = client.scan(cursor, match="user:*", count=1000)
        count += len(keys)
        if cursor == 0:
            break
    print(f"Total user:* keys in Redis: {count}")
    
    # Show sample
    print("\nSample user (user:1):")
    sample = client.hgetall("user:1")
    for field, value in sample.items():
        print(f"  {field.decode()}: {value.decode()}")
    
    print("\nSample user (user:50):")
    sample = client.hgetall("user:50")
    for field, value in sample.items():
        print(f"  {field.decode()}: {value.decode()}")
    
    print("\n✅ Setup complete! You can now run the examples.")
    return 0


if __name__ == "__main__":
    exit(main())
