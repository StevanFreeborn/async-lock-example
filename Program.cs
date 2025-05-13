using System.Globalization;

using Microsoft.Data.Sqlite;

const string connectionString = "Data Source=demo.db";
const string csvFilePath = "products.csv";

await InitializeDatabase();
await ClearProductsTable();
await CreateSampleCsvIfNeeded();

List<Product> products = await GetProductsFromCsv();
Console.WriteLine($"Read {products.Count} products from CSV file.");

Task[] importTasks = new Task[3];

for (int i = 0; i < importTasks.Length; i++)
{
  importTasks[i] = Task.Run(async () =>
  {
    foreach (Product product in products)
    {
      bool exists = await CheckIfProductExists(product.Name);

      if (exists)
      {
        continue;
      }

      await AddProduct(product.Name, product.Price);
    }
  });
}

await Task.WhenAll(importTasks);

long productCount = await GetProductsCount();
Console.WriteLine($"Inserted {productCount} products into the database.");
































async Task<long> GetProductsCount()
{
  using SqliteConnection connection = new(connectionString);
  connection.Open();

  SqliteCommand command = connection.CreateCommand();
  command.CommandText = "SELECT COUNT(*) FROM Products";

  object? result = await command.ExecuteScalarAsync();

  return result is not long count ? throw new InvalidOperationException("Failed to get product count.") : count;
}

async Task CreateSampleCsvIfNeeded()
{
  if (File.Exists(csvFilePath))
  {
    return;
  }

  using StreamWriter writer = new(csvFilePath);

  await writer.WriteLineAsync("Name,Price");
  await writer.WriteLineAsync("Laptop,999.99");
  await writer.WriteLineAsync("Mouse,25.99");
  await writer.WriteLineAsync("Keyboard,45.50");
  await writer.WriteLineAsync("Monitor,249.99");
  await writer.WriteLineAsync("Headphones,75.50");
  await writer.WriteLineAsync("Webcam,89.99");
  await writer.WriteLineAsync("USB Hub,19.99");
  await writer.WriteLineAsync("External Hard Drive,129.99");
  await writer.WriteLineAsync("Microphone,99.99");
  await writer.WriteLineAsync("Laptop Stand,39.99");
  await writer.WriteLineAsync("Wireless Charger,29.99");
  await writer.WriteLineAsync("Portable SSD,149.99");
  await writer.WriteLineAsync("Smartphone,699.99");
  await writer.WriteLineAsync("Tablet,499.99");
  await writer.WriteLineAsync("Smartwatch,199.99");
  await writer.WriteLineAsync("Bluetooth Speaker,129.99");
  await writer.WriteLineAsync("Action Camera,299.99");
}

async Task<List<Product>> GetProductsFromCsv()
{
  List<Product> products = [];
  bool isFirstLine = true;

  await foreach (string line in File.ReadLinesAsync(csvFilePath))
  {
    if (isFirstLine)
    {
      isFirstLine = false;
      continue;
    }

    string[] parts = line.Split(',');

    if (parts.Length < 2 || !decimal.TryParse(parts[1], NumberStyles.Any, CultureInfo.InvariantCulture, out decimal price))
    {
      continue;
    }

    products.Add(new(parts[0].Trim(), price));
  }

  return products;
}


async Task ClearProductsTable()
{
  using SqliteConnection connection = new(connectionString);
  connection.Open();

  SqliteCommand command = connection.CreateCommand();
  command.CommandText = "DELETE FROM Products";
  _ = await command.ExecuteNonQueryAsync();
}

async Task InitializeDatabase()
{
  using SqliteConnection connection = new(connectionString);
  connection.Open();

  SqliteCommand command = connection.CreateCommand();
  command.CommandText = @"
    CREATE TABLE IF NOT EXISTS Products (
      Id INTEGER PRIMARY KEY AUTOINCREMENT,
      Name TEXT NOT NULL,
      Price REAL NOT NULL
    )
  ";

  _ = await command.ExecuteNonQueryAsync();
}

async Task<bool> CheckIfProductExists(string name)
{
  using SqliteConnection connection = new SqliteConnection(connectionString);
  connection.Open();

  SqliteCommand command = connection.CreateCommand();
  command.CommandText = @"SELECT COUNT(*) FROM Products WHERE Name = $name";
  _ = command.Parameters.AddWithValue("$name", name);

  object? result = await command.ExecuteScalarAsync();

  return result is not long count ? throw new InvalidOperationException("Failed to get product count.") : count > 0;
}

async Task AddProduct(string name, decimal price)
{
  using SqliteConnection connection = new SqliteConnection(connectionString);
  connection.Open();

  SqliteCommand command = connection.CreateCommand();
  command.CommandText =
  @"
                    INSERT INTO Products (Name, Price)
                    VALUES ($name, $price)
                ";
  _ = command.Parameters.AddWithValue("$name", name);
  _ = command.Parameters.AddWithValue("$price", price);

  _ = await command.ExecuteNonQueryAsync();
}

internal record Product(string Name, decimal Price);