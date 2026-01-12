"""
Data Generation Script for E-commerce Analytics
Generates synthetic user logs, transactions, and product catalog
for E-commerce platform analysis.
"""
# import necessary libraries
import argparse
import random
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd
from pathlib import Path

# Set random seed for reproducibility
fake = Faker()
Faker.seed(42)
random.seed(42)

# E-commerce Data Generator Class
class EcommerceDataGenerator:
    """Generate synthetic E-commerce data"""
    
    def __init__(self, num_users=1000, num_products=200):
        self.num_users = num_users
        self.num_products = num_products

        # Generate user and product IDs lists
        self.users = [f"user_{i:06d}" for i in range(num_users)]
        self.products = [f"product_{i:04d}" for i in range(num_products)]
        
        # Product categories
        self.categories = [
            "Electronics", "Clothing", "Home & Garden", 
            "Sports", "Books", "Toys", "Beauty", "Food"
        ]
        
        # Brands
        self.brands = {
        "Electronics": ["Samsung", "Sony", "Apple", "Philips"],
        "Clothing": ["Nike", "Adidas", "H&M", "Louis Vuitton"],
        "Home & Garden": ["Ikea", "Bosch", "HomeEase", "GardenPro"],
        "Sports": ["Nike", "Adidas", "Decathlon", "Under Armour"],
        "Books": ["Gallimard", "Penguin", "Hachette", "BookWorld"],
        "Toys": ["Lego", "Playmobil", "Mattel", "ToyBox"],
        "Beauty": ["L'Oréal", "Sephora", "Yves Rocher", "BeautyPlus"],
        "Food": ["Nestlé", "Danone", "Carrefour", "Foodies"]
}

        
        # Page URLs or differents pages on web site
        self.pages = [
            "/", "/products", "/cart", "/checkout", "/account", "/orders", "/returns",
            "/search", "/category/electronics", "/category/clothing",
            "/category/home", "/category/sports", "/category/books", "/category/toys", 
            "/category/beauty", "/category/food", "/offers", "/faq", "/about", "/contact"
        ]
        
        # Actions
        self.actions = ["view", "click", "add_to_cart", "remove_from_cart", "search"]
        
        # Device types
        self.devices = ["Desktop", "Mobile", "Tablet"]

    # Generate product catalog
    def generate_products(self):
        """Generate product catalog"""
        print("Generating product catalog...")
        
        products_data = []
        for product_id in self.products:
            category = random.choice(self.categories)
            products_data.append({
                "product_id": product_id,
                "product_name": fake.catch_phrase(),
                "category": random.choice(list(self.categories)),
                "price": round(random.uniform(9.99, 999.99), 2),
                "brand": random.choice(self.brands[category]),
                "stock_quantity": random.randint(0, 1000),
                "rating": round(random.uniform(1.0, 5.0), 1)
            })
        
        return pd.DataFrame(products_data)
    
    # Generate user navigation logs
    def generate_user_logs(self, num_logs=10000, days=30):
        """Generate user navigation logs"""
        print(f"Generating {num_logs:,} user logs...")
        
        logs_data = []
        start_date = datetime.now() - timedelta(days=days)
        
        for _ in range(num_logs):
            user_id = random.choice(self.users)
            timestamp = start_date + timedelta(
                days=random.randint(0, days),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
            
            # Generate session ID (users can have multiple sessions)
            session_id = f"session_{fake.uuid4()[:8]}"
            
            logs_data.append({
                "user_id": user_id,
                "timestamp": timestamp,
                "page_url": random.choice(self.pages),
                "session_id": session_id,
                "action": random.choices(
                    self.actions, 
                    weights=[50, 30, 15, 3, 2]  # view is most common
                )[0],
                "device_type": random.choices(
                    self.devices,
                    weights=[40, 50, 10]  # mobile slightly more common
                )[0],
                "duration_seconds": random.randint(5, 600)
            })
        
        df = pd.DataFrame(logs_data)


        # Sort by timestamp
        df = df.sort_values("timestamp").reset_index(drop=True)
        return df
    
    # Generate transaction data
    def generate_transactions(self, num_transactions=5000, days=30):
        """Generate transaction data"""
        print(f"Generating {num_transactions:,} transactions...")
        
        transactions_data = []
        start_date = datetime.now() - timedelta(days=days)
        
        # Select subset of users who actually purchase (not everyone buys)
        purchasing_users = random.sample(self.users, int(self.num_users * 0.3))
        
        for _ in range(num_transactions):
            user_id = random.choice(purchasing_users)
            timestamp = start_date + timedelta(
                days=random.randint(0, days),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            
            # Some users buy multiple items in one transaction
            num_items = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3])[0]
            
            for _ in range(num_items):
                product_id = random.choice(self.products)
                quantity = random.choices([1, 2, 3], weights=[70, 20, 10])[0]
                price = round(random.uniform(9.99, 999.99), 2)
                
                transactions_data.append({
                    "transaction_id": fake.uuid4(),
                    "user_id": user_id,
                    "product_id": product_id,
                    "quantity": quantity,
                    "unit_price": price,
                    "amount": round(price * quantity, 2),
                    "timestamp": timestamp,
                    "payment_method": random.choice(["credit_card", "paypal", "debit_card", "gift_card"]),
                    "status": random.choices(
                        ["completed", "pending", "cancelled", "refunded"],
                        weights=[85, 8, 5, 2]
                    )[0]
                })
        
        df = pd.DataFrame(transactions_data)
        df = df.sort_values("timestamp").reset_index(drop=True)
        return df
    
    # Save generated data to CSV
    def save_data(self, output_dir, num_logs, num_transactions):
        """Generate and save all datasets"""
        print(f"\n Starting data generation...")
        print(f"Configuration:")
        print(f"   - Users: {self.num_users:,}")
        print(f"   - Products: {self.num_products:,}")
        print()
        
        
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        products_df = self.generate_products()
        logs_df = self.generate_user_logs(num_logs)
        transactions_df = self.generate_transactions(num_transactions)
        
        # Save as CSV
        print(f"\n Saving data to {output_dir}/")
        products_df.to_csv(f"{output_dir}/products.csv", index=False)
        logs_df.to_csv(f"{output_dir}/user_logs.csv", index=False)
        transactions_df.to_csv(f"{output_dir}/transactions.csv", index=False)
        
        # Print summary
        print("\n Data generation complete!")
        print("\n Summary:")
        print(f"   - Products: {len(products_df):,} rows")
        print(f"   - User Logs: {len(logs_df):,} rows")
        print(f"   - Transactions: {len(transactions_df):,} rows")
        print(f"\n Files saved:")
        print(f"   - {output_dir}/products.csv")
        print(f"   - {output_dir}/user_logs.csv")
        print(f"   - {output_dir}/transactions.csv")
        
        return products_df, logs_df, transactions_df


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(
        description="Generate synthetic e-commerce data for PySpark analytics"
    )
    parser.add_argument(
        "--users", 
        type=int, 
        default=1000,
        help="Number of unique users (default: 1000)"
    )
    parser.add_argument(
        "--products", 
        type=int, 
        default=200,
        help="Number of products (default: 200)"
    )
    parser.add_argument(
        "--logs", 
        type=int, 
        default=10000,
        help="Number of user logs (default: 10000)"
    )
    parser.add_argument(
        "--transactions", 
        type=int, 
        default=5000,
        help="Number of transactions (default: 5000)"
    )
    parser.add_argument(
        "--output", 
        type=str, 
        default="data/raw",
        help="Output directory (default: data/raw)"
    )
    
    args = parser.parse_args()
    
    # Generate data
    generator = EcommerceDataGenerator(num_users=args.users, num_products=args.products)
    generator.save_data(output_dir=args.output, num_logs=args.logs, num_transactions=args.transactions)
    
    # Override default log/transaction counts if specified
    #generator.generate_user_logs = lambda: generator._generate_user_logs(num_logs=args.logs)
    generator.generate_transactions = lambda: generator.generate_transactions(num_transactions=args.transactions)
    
    #generator.save_data(output_dir=args.output)
    logs_df = generator.generate_user_logs(num_logs=args.logs)
    
    print("\n Ready to start PySpark analysis!")


if __name__ == "__main__":
    main()