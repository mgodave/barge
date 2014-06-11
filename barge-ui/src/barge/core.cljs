(ns barge.core
    (:require [om.core :as om :include-macros true]
              [om.dom :as dom :include-macros true]
              [clojure.string :as string]))

(enable-console-print!)

(def node-state
  "the state of the system is a map of node ids to a list of log messages received
ordered from newest to latest"
  (atom
    {:node1
       [{:msg "this is a message" :timestamp "2014-06-11"}
        {:msg "this is another message" :timestamp "2014-06-11"}]
     :node2
       [{:msg "this is a message" :timestamp "2014-06-11"}]
     :node3
       [{:msg "this is a message" :timestamp "2014-06-11"}
       {:msg "this is a message" :timestamp "2014-06-11"}
       {:msg "this is another message" :timestamp "2014-06-11"}]
     }))

(defn update-msgs [node msg]
  "update the messages list for given node, appending a new message

  state is updated asynchronously"
  (swap! node-state (fn [st]
                      (update-in st [(keyword node)] #(conj % msg)))))

(defn node-view [id]
  (fn [app owner]
    (reify
      om/IRender
      (render [_]
        (dom/div #js {:id id}
          (dom/h1 nil id)
          (apply dom/ul nil
            (map #(dom/li nil (:msg %)) ((keyword id) app))))))))


(om/root (node-view "node1")
         node-state
  {:target (. js/document (getElementById "node1"))})

(om/root (node-view "node2")
         node-state
  {:target (. js/document (getElementById "node2"))})

(om/root (node-view "node3")
         node-state
  {:target (. js/document (getElementById "node3"))})

