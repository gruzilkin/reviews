from service.Worker import Worker

if __name__ == '__main__':
    with Worker() as worker:
        worker.doWork()
