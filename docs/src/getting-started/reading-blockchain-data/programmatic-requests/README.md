---
description: >-
  Build scripts using Axios or Requests, still calling read RPC methods but
  using more involved methods, e.g. eth_call
---

# Programmatic Requests

We can also write a simple script to make cURL requests. This will allow us to build around the RPC responses. This will make it easier to call different endpoints with different arguments and handle data that we receive from the RPC calls.

First, let's set up the typescript project.

1. check node and npm (node package manager) versions:&#x20;

In your terminal, enter:

```
node --version
npm --version
```

It should give something like:&#x20;

```
v18.17.1
10.1.0
```

If these packages are not installed, run:

```
sudo apt install nodejs npm
```

2. Create a project directory and navigate into it:

```
mkdir telSimpleCalls
cd telSimpleCalls
```

3. Open a new VsCode (or alternative IDE) window of your directory:

```
code .
```

4. Initialise a new project:

```
npm init -y
```

5. As we will need typescript and axios packages, we install them using:

```
npm install typescript axios
```

6. Initialise the TypeScript configuration:

```bash
npx tsc --init
```

This command creates a `tsconfig.json` file in your project directory. You can leave the settings as default.

You should now have your TypeScript project set up! Let's get into writing some scripts!
