(defproject barge-jepsen "0.1.0-alpha2-SNAPSHOT"
  :description "Jepsen (Call me Maybe) Tests for Barge's linearizability"
  :dependencies [[org.clojure/clojure "1.6.0-beta1"]
                 [org.clojure/tools.logging "0.2.6"]
                 [jepsen/jepsen "0.0.3-SNAPSHOT"]
                 [org.robotninjas.barge/barge-store "0.1.0-alpha2-SNAPSHOT"]])
