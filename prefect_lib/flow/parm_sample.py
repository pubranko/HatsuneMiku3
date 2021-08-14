from prefect import task, Flow, Parameter

@task
def print_plus_one(x,y):
    print(x + 1)
    print(type(x))
    print(y)
    print(type(y))
    with open('logs/sample.txt',mode='w') as f:
        f.write(str(x+1))
        f.write(y)
        f.write(str(type(y)))


with Flow('Parameterized Flow') as flow:
    x = Parameter('parm1', default = 3)
    y = Parameter('parm2', default = 'abc')
    flow.add_task(x)
    flow.add_task(y)
    print_plus_one(x=x,y=y)

flow.run(parameters=dict(parm1=1)) # prints 2
flow.run(parameters={'parm1':100}) # prints 101
flow.run() #prints 3
