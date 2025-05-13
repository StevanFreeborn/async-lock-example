internal class AsyncLock : IDisposable
{
  private readonly SemaphoreSlim _semaphore = new(1, 1);

  public async Task<IDisposable> LockAsync()
  {
    await _semaphore.WaitAsync();
    return new Releaser(this);
  }

  public void Dispose()
  {
    _semaphore.Dispose();
  }

  private class Releaser(AsyncLock asyncLock) : IDisposable
  {
    private readonly AsyncLock _asyncLock = asyncLock;

    public void Dispose()
    {
      _ = _asyncLock._semaphore.Release();
    }
  }
}