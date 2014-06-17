(ns barge.core
    (:require [om.core :as om :include-macros true]
              [om.dom :as dom :include-macros true]
              [clojure.string :as string])
  (:import [goog.net WebSocket]
           goog.net.WebSocket.EventType
           [goog.events EventHandler]))

(enable-console-print!)

(def node-state
  "the state of the system is a map of node ids to a connection state and  list of log messages received
ordered from newest to latest"
  (atom
    {:node1 {
      :uri "ws://127.0.0.1:56789/events"
      :ws nil
      :msgs [{:msg "this is a message" :timestamp "2014-06-11"}
        {:msg "this is another message" :timestamp "2014-06-11"}]
      }
     :node2 {
      :uri "ws://127.0.0.1:56790/events"
      :ws nil
      :msgs [{:msg "this is a message" :timestamp "2014-06-11"}]
      }
     :node3 {
      :uri "ws://127.0.0.1:56791/events"
      :ws nil
      :msgs [{:msg "this is a message" :timestamp "2014-06-11"}
       {:msg "this is a message" :timestamp "2014-06-11"}
       {:msg "this is another message" :timestamp "2014-06-11"}]
      }
     }))

(defn update-msgs [node msg]
  "update the messages list for given node, appending a new message

  state is updated asynchronously"
  (swap! node-state (fn [st]
                      (update-in st [(keyword node) :msgs] #(conj % {:msg msg :timestamp "2014-06-11"})))))

;; message handlers
(defn on-msg [node e]
  "Handles messages from the server, that is notifications regarding the state of some node"
  (let [msg    (.-message e)
        ;; msg is a Blob which should be decoded by a js/FileReader
        ;; see https://developer.mozilla.org/en-US/docs/Web/API/Blob
        reader (js/FileReader.)]
        ;; callback updates messages with whatever text is contained in the blob
        (.addEventListener reader "loadend" #(update-msgs node (.-result reader)))
        ;; text is assumed to be encoded in UTF-8 which BTW is the default
        (.readAsText reader msg "UTF-8"))
    )

;; TODO better notification of node's state
;; we want to add a notification area within each node that displays until dismissed
;; notification messages
(defn on-open [e]
  (.log js/console (str "open " e)))

(defn on-close [e]
  (.log js/console (str "close " e)))

(defn on-error [e]
  (.log js/console (str "error " (.-type e))))

;; connect to server and register listeners
(defn connect [st node]
  "Connect to given node id and updates state accordingly"
  (let [ws     (WebSocket.)
        eh     (EventHandler.)
        uri    (:uri ((keyword node) st))]

    (.listen eh ws EventType.MESSAGE (partial on-msg node))
    (.listen eh ws EventType.OPENED on-open)
    (.listen eh ws EventType.CLOSED on-close)
    (.listen eh ws EventType.ERROR on-error)
    (.open ws uri)
    (assoc-in st [(keyword node) :ws] ws)))

(defn disconnect [st node]
  "disconnect node and updates state accordingly"
  (let [ws (:ws ((keyword node) st))]
    (.close ws)
    (assoc-in st [(keyword node) :ws] nil)
    ))


(defn toggle-connect [owner node connected]
  "connect/disconnect given node and updates state of owner accordingly"
  (swap! node-state (fn [st]
                      (if connected
                        (disconnect st node)
                        (connect st node))))
  (om/set-state! owner :connected (not connected))
  )

(defn node-view [id]
  (fn [app owner]
    (reify
      om/IInitState
      (init-state [_]
        {:connected false})
      om/IRenderState
      (render-state [_ {:keys [connected]}]
        (dom/div #js {:id id}
          (dom/h1 nil id)
          (dom/button
             #js {:onClick #(toggle-connect owner id (om/get-state owner :connected))}
            (if connected
              "Disconnect"
             "Connect"
              ))
          (apply dom/ul nil
            (map #(dom/li nil (:msg %)) (:msgs ((keyword id) app)))))))))


(defn set-root [name]
  (om/root (node-view name)
           node-state
    {:target (. js/document (getElementById name))}))

(set-root "node1")
(set-root "node2")
(set-root "node3")


