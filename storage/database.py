from db_setup import engine, Base


def create_tables():
    Base.metadata.create_all(engine)
    print("All tables created successfully.")

def drop_tables():
    Base.metadata.drop_all(engine)
    print("All tables dropped successfully.")

if __name__ == "__main__":
    create_tables() 
