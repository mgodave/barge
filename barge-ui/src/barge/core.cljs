(ns barge.core
    (:require [om.core :as om :include-macros true]
              [om.dom :as dom :include-macros true]
              [clojure.string :as string])
  (:import [goog.net WebSocket]
           goog.net.WebSocket.EventType
           [goog.events EventHandler]))

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


(defn set-root [name]
  (om/root (node-view name)
           node-state
    {:target (. js/document (getElementById name))}))

(set-root "node1")
(set-root "node2")
(set-root "node3")

;; the current websocket
(def websocket (atom nil))

;; message handlers
(defn on-msg [e]
  (js/alert (.-message e)))

(defn on-open [e]
  (js/alert e))

(defn on-close [e]
  (js/alert e))

(defn on-error [e]
  (js/alert (.-type e)))


;; connect to server and register listeners
(defn connect [_]
  (let [ws (WebSocket.)
        eh (EventHandler.)]
    (reset! websocket ws)
    (.listen eh ws EventType.MESSAGE on-msg)
    (.listen eh ws EventType.OPENED on-open)
    (.listen eh ws EventType.CLOSED on-close)
    (.listen eh ws EventType.ERROR on-error)
    (.open ws "ws://127.0.0.1:8080/events")))

(defn disconnect [_]
  (.close @websocket)
  (reset! websocket nil))
