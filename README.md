# `parallex`
## Introduction
A queue with dependencies

## Usage

```
from parallex import run

ret = run(number_of_workers = 4, specf = "spec.yml", dataf = "data.yml")
```

## Spec
Each task is given a dict called `data`.

### `map`
The `map` task reads a list `coll` from `data` and applies a list of subtasks to each member of the list. The members will be assigned to `var` in `data` passed to those tasks

```
type: map
coll: <variable name for collection>
var: <variable name>
sub: <subtasks>
```

### `top`

The `top` task toplogically sorts subtasks. It reads the `depends_on` property of subtasks, which has format:

```
<task name>: [<param>, ..., <param>]
...
<task name>: [<param>, ..., <param>]
```
The result of a task will be assign the parameters that it maps to.

### `python`

The `python` task runs a python function. It reads parameters from `data`.
```
type: python
name: <name>
mod: <module>
func: <function>
params: <parameters>
depends_on: <dependencies>
```


## Data

data can be arbitrary yaml

