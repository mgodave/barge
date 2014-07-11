(defproject barge-ui "0.1.0-SNAPSHOT"
  :description "A minimal interface to barge REST API for demo purpose"
  :url "http://github.com/mgodave/barge"

  :jvm-opts ^:replace ["-Xmx1g" "-server"]

  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/clojurescript "0.0-2173"]
                 [org.clojure/core.async "0.1.267.0-0d7780-alpha"]
                 [org.clojure/tools.nrepl "0.2.3"]
                 [om "0.6.4"]]

  :plugins [[lein-cljsbuild "1.0.2"]]

  :source-paths ["src"]

  :cljsbuild {
    :builds [{:id "dev"
              :source-paths ["src"]
              :compiler {
                :output-to "out/app.js"
                :output-dir "out"
                :pretty-print true
                :optimizations :none
                :source-map true}}
             {:id "release"
              :source-paths ["src"]
              :compiler {
                :output-to "app.js"
                :optimizations :advanced
                :elide-asserts true
                :pretty-print false
                :output-wrapper false
                :preamble ["react.min.js"]
                :externs ["react/externs/react.js"]}}]})
