# Datly customization extension 

## Introduction

In some scenario you may consider blending datly with customized go code for both reader and executor service.
This is achieved by registering custom data type in xdatly registry, once that done you can use/invoke any registered type method.

Datly in this mode requires custom datly binary, which supplemental changes managed by datly go plugin.
In this scenario once initial datly binary is deployed, any logic changes in dsql or struct behaviour require only rule and plugin folder deployment.
Datly use go plugin architecture, thus if any go.mod dependency changes it requires custom datly binary redeployment.

#### Custom datly project initialization

In this mode you define datly project with customized go module

```bash
datly initExt -p=projectPath -n=nameOfYourGoModule
```
The following project structure get generated

```bash
 Project Root
  | .build
  |    - datly
  | - dsql
  | 
  | - pkg 
  |    |
  |    |- dependency
  |    |    | - init.go
  |   go.mod
```

where
 - go.mod holds name of you go project with xdatly dependencies 
 - '.build' folder holds datly project files and go.mod with replacement: github.com/viant/xdatly/extension => ${projectPath}/pkg
 - 'pkg/depenency/init.go' contains default imports with customized go struct xdatly registration


#### Generating dsql for patch,put or post operation

To add prepare/generate executor (put/patch/post) rule run the following command
```bash
 gen -o=patch -c='ci_ads|mysql|root:dev@tcp(127.0.0.1:3306)/mydb?parseTime=true' \
 -s=entity_patch.sql \
 -g=xxxx \
 -m=$projectPath
```

would add rule to dsql folder and go struct to pkg folder



#### Convert dsql into datly rule project


#### Building datly binary

```bash
datly build -h
```
###### Standalone app
```bash
  datly build -p=$ProjectPath -r=standalone -d='$ProjectPath/bin'  -o=darwin -a=arm64
```

###### AWS Lambda

```bash
  datly build -p='$ProjectPath' -r='lambda/url' -d='$ProjectPath/bin'  -o=linux -a=amd64
```

#### Building datly plugin

```bash
datly plugin -p='$ProjectPath' -d='$ProjectPath/plugins/'
```


