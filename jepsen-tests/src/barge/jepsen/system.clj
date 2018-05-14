(ns barge.jepsen.system
  (:require [clojure.java.io        :as io]
            [clojure.string         :as str]
            [clojure.tools.logging  :refer [info]]
            [jepsen.client          :as client]
            [jepsen.control         :as c]
            [jepsen.control.net     :as net]
            [jepsen.db              :as db]
            [jepsen.os.debian       :as debian]
            [jepsen.util            :refer [meh timeout]]))


;; GAV coordinates and repository where to find barge
(def ^:dynamic *barge-version* "0.1.0-alpha2")
(def ^:dynamic *barge-groupid* "org.robotninjas.barge")
(def ^:dynamic *barge-artifactid* "barge-store")

(def ^:dynamic *barge-repository* "https://dl.dropboxusercontent.com/u/2060057/maven/")

(defn barge-jar []
  (str *barge-artifactid* "-" *barge-version* ".jar"))

(defn barge-uri []
    (str *barge-repository*
         (str/replace *barge-groupid* "." "/") "/"
         *barge-artifactid* "/"
         *barge-version* "/"
         (barge-jar)))

(defn barge-cluster-config
  "Output configuration of barge cluster in a format suitable for populating a barge.conf
  file, eg:

```
n1=http://1.2.3.4:8081
n2=http://1.2.3.5:8081
n3=http://1.2.3.6:8081
```

  * nodes: A sequence of nodes namespace
  * fn-node-uri: Function to retrieve the node's IP given its name
  "
  [nodes fn-node-ip]
  (str/join "\n"
            (map (fn [node]
         (str node "=http://" (fn-node-ip node) ":56789/"))
       nodes
  )))

(barge-cluster-config ["n1" "n2" "n3"] (fn [_] "http://1.2.3.5:8081") )

(def db
  (reify db/DB
    (setup! [_ test node]
      (c/su
         (info node "installing barge-store")
         (c/cd "/tmp"
           (info node "installing java")
           (c/exec :apt-get :install :-y :openjdk-7-jre-headless)

           (info node "installing barge " *barge-version*)
           (c/exec :wget :-c (barge-uri))

           (info node "configuring barge")
           (c/exec :echo
                (barge-cluster-config (:nodes test) #(c/on % (net/local-ip)))
                :> "barge.conf")

           (info node "starting barge")
           (c/exec :java :-jar (symbol (barge-jar)) node)

       )))

    (teardown! [_ test node]
      (c/su
       (info node "removing barge-store")
       (c/exec :java :-cp (barge-jar) :stop node)

       (c/exec :rm :-fr (str "log" node))
       ))))


(c/with-ssh
  {:username "vagrant"
   :password "vagrant"}
  (c/on "192.168.43.2"
      (db/setup! db {:nodes ["192.168.43.2"]} "192.168.43.2")))
