[![Build Status](https://travis-ci.com/RENCI/tx-parallex.svg?branch=master)](https://travis-ci.com/RENCI/tx-parallex)

# `parallex`
## Introduction
A queue with dependencies

## Usage

```
from tx.parallex import run

ret = run_python(number_of_workers = 4, pyf = "spec.py", dataf = "data.yml")
```

## Spec

`tx-parallex` specs can be written in YAML or a Python-like DSL. The Python-like DSL is translated to YAML by `tx-parallex`. Each object in a spec specifies a task. When the task is executed, it is given a dict called `data`. The pipeline will return a dictionary.

### YAML
Assuming you have a function `sqr` which increment defined in module `math` which squares its argument and return the result.

```
def sqr(x):
  return x * x
```

#### `let`
The `let` task sets `data` for its subtask. It adds new var value pairs into `data` within the scope of its subtask, and executes that task.

Syntax:
```
type: let
obj: 
  <var>: <value>
  ...
  <var>: <value>
sub: <subtask>
```

Example:
```
type: let
obj:
  a: 1
sub:
  type: python
  name: y
  mod: math
  func: sqr
  params: 
    x:
      name: a
  ret:
  - b
```



### `map`
The `map` task reads a list `coll` from `data` and applies a subtask to each member of the list. The members will be assigned to `var` in `data` passed to those tasks

Syntax:
```
type: map
coll: <value>
var: <variable name>
sub: <subtask>
```

`<value>` is an object of the form
Reference an entry in `data`
```
"name": <variable name>
```
Reference the name of a task
```
"depends_on": <task name>
```
Constant
```
"data": <constant>
```

Example:
```
type: map
coll: 
- 1
- 2
- 3
var: a
sub:
  type: python
  name: y
  mod: math
  func: sqr
  params: 
    x:
      name: a
  ret:
  - b
```

### `python`

You can use any python module.

The `python` task runs a python function. It reads parameters from `data`.

Syntax:
```
type: python
name: <name>
mod: <module>
func: <function>
params: <parameters>
ret: <returns>
```

`<parameters>` is an object of the form
```
<param> : <value>
...
<param> : <value>
```

`ret` specify a list of names that will map to the return value of task. The pipeline will return a dictionary containing these names. When a task appears under a `map` task, each name is prefix with the index of the element in that collection as following 

```
<index>.<name>
```
For nested maps, the indices will be chained together as followings
```
<index>. ... .<index>.<name>
```

Example:
```
  type: python
  name: y
  mod: math
  func: sqr
  params: 
    x:
      name: a
  ret:
  - b
```
### `top`

The `top` task toplogically sorts subtasks based on their dependencies and ensure the tasks are executed in parallel in the order compatible with those dependencies. 

Syntax:
```
type: top
sub: <subtasks>
```

It reads the `depends_on` properties of subtasks.

Example:
```
type: top
sub:
- type: python
  name: y
  mod: math
  func: sqr
  params: 
    x:
      name: a
- type: python
  name: z
  mod: math
  func: sqr
  params: 
    x:
      depends_on: y
  ret:
  - c
```
## Python
A dsl block contains a subset of python.

Available syntax:

### assignment
```
<var> = <const> | <list> | <dict>
```
This translates to `let`.

### function application
```
<var> = <module>.<func>(<param>=<arg>, ...)
```
This translate to `python`.
where `<var>` is `name`
`<arg>` is translated to `name`, `depends_on`, or `data` depending on its content. It is translated to `depends_on` whenever there is an assignment to it in the code block, even after it.

### parallel for

```
for <var> in <value>:
    ...
```
This translates to `map`.

### return
```
return <dict>
```
This translates to `ret` in `python`. The key of the dict will be translated to the list in `ret`.



## Data

data can be arbitrary yaml

