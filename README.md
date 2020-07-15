[![Build Status](https://travis-ci.com/RENCI/tx-parallex.svg?branch=master)](https://travis-ci.com/RENCI/tx-parallex)

# `parallex`
## System Requirements
Python >= 3.8
## Install
1. Clone the repo
2. Easy install instructions:
```
# Create a virtual environment called 'px'
conda create -n px python=3.8
# start-up the environment you just created
conda activate px
# install the rest of the tx-parallex pre-requirements
pip install -r requirements.txt
```
3. Test
```
# run the tests, a number of test 'specs'
PYTHONPATH=src pytest -x -vv --full-trace -s --timeout 10
# deactivate the environment (if desired)
conda deactivate
```

## Introduction
A queue with dependencies

## Usage

```
from tx.parallex import run_python

ret = run_python(number_of_workers = 4, pyf = "spec.py", dataf = "data.yml")
```

## Spec

`tx-parallex` specs can be written in YAML or a Python-like DSL. The Python-like DSL is translated to YAML by `tx-parallex`. Each object in a spec specifies a task. When the task is executed, it is given a dict called `data`. The pipeline will return a dictionary.

### YAML
Assuming you have a function `sqr` defined in module `math` which returns the square of its argument.

```
def sqr(x):
  return x * x
```

#### `let`
The `let` task sets `data` for its subtask. It adds a new var value pair into `data` within the scope of its subtask, and executes that task.

Syntax:
```
type: let
var: <var>
obj: <value>
sub: <subtask>
```

Example:
```
type: let
var: a
obj:
  data: 1
sub:
  type: python
  name: y
  mod: math
  func: sqr
  params: 
    x:
      name: a
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

`<value>` is an object of the form:

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
  data:
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
```

### `cond`
The `cond` task reads a boolean value and if it is true then it executes the `then` task otherwise it executes the `else` task.

Syntax:
```
type: cond
on: <value>
then: <subtask>
else: <subtask>
```

Example:
```
type: cond
on: 
  data:
    true
then:
  type: ret
  var: x
  obj:
    data: 1
else:
  type: ret
  var: x
  obj:
    data: 0
```



### `python`

You can use any Python module.

The `python` task runs a Python function. It reads parameters from `data`. The return value must be pickleable.

Syntax:
```
type: python
name: <name>
mod: <module>
func: <function>
params: <parameters>
```

`<parameters>` is an object of the form:
```
<param> : <value>
...
<param> : <value>
```
where `<param>` can be either name or position.

Example:
```
  type: python
  name: y
  mod: math
  func: sqr
  params: 
    x:
      data: 1
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
      data: 1
- type: python
  name: z
  mod: math
  func: sqr
  params: 
    x:
      depends_on: y
```

### `ret`
`ret` specify a name that will map to a value. The pipeline will return a dictionary containing these names. When a task appears under a `map` task, each name is prefix with the index of the element in that collection as following 

```
<index>.<name>
```
For nested maps, the indices will be chained together as followings
```
<index>. ... .<index>.<name>
```

Syntax:
```
type: ret
var: <var>
obj: <value>
```

Example:
```
type: ret
var: x
obj: 
    name: z
```

## Python
A dsl block contains a subset of Python.

Available syntax:

### import

```
from <module> import *
from <module> import <func>, ..., <func>
```

import names from module

`<module>` absolute module names

### assignment
```
<var> = <const>
```
where
```
<const> = <integer> | <number> | <boolean> | <string> | <list> | <dict>
```

This translates to `let`.

Example:
```
a = 1
y = sqr(x=a)
return {
  "b": y
}
```

### function application

```
<var> = [<module>.]<func>(<param>=<expr>, ...) | <expr>
```

This translate to `python`.
where `<var>` is `name`
`<expr>` is

```
<expr> = <expr> <binop> <expr> | <expr> <boolop> <expr> | <expr> <compare> <expr> | <unaryop> <expr> | <var> | <const>
```

`<binop>`, `<boolop>` and `<compare>` and `<unaryop>` are python BinOp, BoolOp, Compare, and UnaryOp. `<expr>` is translated to a set of assignments, `name`, `depends_on`, or `data` depending on its content. It is translated to `depends_on` whenever there is an assignment to it in the code block, even after it.

Example:
```
y = math.sqr(1)
z = math.sqr(y)
return {
  "c": z
}
```

### parallel for

```
for <var> in <expr>:
    ...
```
This translates to `map`.

Example:

```
for a in [1, 2, 3]:
  y = math.sqr(a)
  return {
    "b": y
  }
```

### if
```
if <expr>:
    ...
else:
    ...
```

This translates to `cond`.

Example:
```
if z:
    return {
        "x": 1
    }
else:
    return {
        "x": 0
    }
```

The semantics of if is different from python, variables inside if is not visible outside
### return
```
return <dict>
```
This translates to `ret`. The key of the dict will be translated to the var in `ret`.

Example:
```
y = math.sqr(1)
return {
  "b": y
}
```

## Data

data can be arbitrary yaml

