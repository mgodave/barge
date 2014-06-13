# Minimalist Demo UI

This HTML UI is meant to display in real time the state of each (configured) node in a Raft cluster:
* The node's name, IP address and port
* The node's state: active, down
* The list of messages output by this node

# Developing

This UI is developed using [clojurescript]() and more precisely [om](https://github.com/swannodette/om) which is a cljs wrapper
over [Facebook's React](http://facebook.github.io/react/). Follow the instructions below to get a development environment using
[LightTable](http://lighttable.com).

* Open LightTable (note that if you are behind a proxy, you need to setup the `http_proxy` environment variable for the
  LightTable process. I could not find how to do this on Mac OS X so this means I need to launch the `.app` from the command-line)
* `Open Folder` and select *barge-ui*
* Click on the `test.html` file to open it
* Type `Cmd+Enter` in the file: This should open a new tab displaying embeded LightTable browser.
* In the directory `barge-ui`, starts clojurescript compiler in `auto` mode to activate compilation on each change. The following
  command activate cljsbuild compilation for the `dev` build (there is a `prod` build which is currently unused):

    > $ lein cljsbuild auto dev
    > Compiling ClojureScript.
    > Compiling "app.js" from ["src"]...
    > Successfully compiled "app.js" in 3.752 seconds.

* Refresh the embedded browser, you should see the skeletal UI for 3 nodes displayed
* You can now open and hack the `src/barge/core.cljs`: Saving it will trigger recompilation. To update display, refresh
  browser. Evaluating a form in the `core.cljs` will be done in the context of the embedded browser, so it may impact immediately
  the UI
