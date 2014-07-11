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
      :msgs []
      }
     :node2 {
      :uri "ws://127.0.0.1:56790/events"
      :ws nil
      :msgs []
      }
     :node3 {
      :uri "ws://127.0.0.1:56791/events"
      :ws nil
      :msgs []
      }
     }))

(defn parse-msg
  "parse message into a clojure object."
  [msg]
  (cond
   (string? msg)
   (js->clj (JSON/parse msg) :keywordize-keys true)

   true
   msg))

(parse-msg "{\"foo\" : \"bar\"}")

(defn coalesce
  "prepend msg to msgs, coalescing msgs which differ only by their timestamps"
  [msg msgs]
  (cond
    (nil? msgs)
    [msg]

    true
    (let [m1 (assoc-in msg [:timestamp] nil)
          m2 (assoc-in (first msgs) [:timestamp] nil)]
      (if (= m1 m2)
        (cons msg (rest msgs))
      (cons msg msgs))
      )
    )
  )

(defn update-msgs [node msg]
  "update the messages list for given node, appending a new message

  state is updated asynchronously"
  (swap! node-state (fn [st]
                      (update-in st [(keyword node) :msgs] (partial coalesce (parse-msg msg))))))

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
  (if-let [ws (:ws ((keyword node) st))]
    (do (.close ws)
        (assoc-in st [(keyword node) :ws] nil))))


(defn toggle-connect [owner node connected]
  "connect/disconnect given node and updates state of owner accordingly"
  (swap! node-state (fn [st]
                      (if connected
                        (disconnect st node)
                        (connect st node))))
  (om/set-state! owner :connected (not connected)))

(defn node-view [id]
  (fn [app owner]
    (reify
      om/IInitState
      (init-state [_]
        {:connected false
         :count     15})
      om/IRenderState
      (render-state [_ {:keys [connected count]}]
        (dom/div #js {:id id}
          (dom/button
             #js {:onClick #(toggle-connect owner id (om/get-state owner :connected))
                  :className "pure-button pure-button-primary"}
            (if connected
              (str "Disconnect " id)
             (str "Connect " id)
              ))
          (dom/table #js {:className "pure-table"}
              (dom/thead nil
                (dom/tr nil
                  (dom/th nil "Timestamp")
                  (dom/th nil "Message")))
              (apply dom/tbody nil
                 (map format-msg
                     (take count (:msgs ((keyword id) app)))))))))))

(defn format-msg
 "format a message received from a Raft instance as a DOM node"
   [msg]
  (dom/tr nil
       (dom/td nil (:timestamp msg))
       (dom/td #js {:className (:type msg)}
               (case (:type msg)
                 "init"
                 "INIT"

                 "stateChange"
                 (str (:from msg) " -> " (:to msg))

                 "invalidTransition"
                 (str (:expected msg) " vs. " (:actual msg))

                 "stopping"
                 "STOP"

                 "appendEntries"
                 (str (count (:entriesList (:entries msg))) " entries, term: " (:term (:entries msg)))

                 "requestVote"
                 (str (:candidateId (:vote msg)) ", term: " (:term (:vote msg)))

                 "commit"
                 (str (:operation msg))

                 "" ))))


(defn set-root
  [name] (om/root (node-view name)
             node-state
          {:target (. js/document (getElementById name))}))

(set-root "node1")
(set-root "node2")
(set-root "node3")


