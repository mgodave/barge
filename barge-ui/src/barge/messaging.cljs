(ns barge.messaging
  "Provides messaging logic between UI and WS server."

  (:import [goog.net WebSocket]
           goog.net.WebSocket.EventType
           [goog.events EventHandler]))


(defn- parse-msg
  "parse message into a clojure object."
  [msg]
  (cond
   (string? msg)
   (js->clj (JSON/parse msg) :keywordize-keys true)

   true
   msg))

(defn- coalesce
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

(defn- update-msgs [state node msg]
  "update the messages list for given node, appending a new message

  state is updated asynchronously"
  (swap! state (fn [st]
                        (update-in st [(keyword node) :msgs] (partial coalesce (parse-msg msg))))))
;; message handlers
(defn- on-msg [state node e]
  "Handles messages from the server, that is notifications regarding the state of some node"
  (let [msg    (.-message e)
        ;; msg is a Blob which should be decoded by a js/FileReader
        ;; see https://developer.mozilla.org/en-US/docs/Web/API/Blob
        reader (js/FileReader.)]
        ;; callback updates messages with whatever text is contained in the blob
        (.addEventListener reader "loadend" #(update-msgs state node (.-result reader)))
        ;; text is assumed to be encoded in UTF-8 which BTW is the default
        (.readAsText reader msg "UTF-8"))
    )

;; TODO better notification of node's state
;; we want to add a notification area within each node that displays until dismissed
;; notification messages
(defn- on-open [e]
  (.log js/console (str "open " e)))

(defn- on-close [e]
  (.log js/console (str "close " e)))

(defn- on-error [e]
  (.log js/console (str "error " (.-type e))))

(defn connect [st node node-state]
  "Connect to given node id and updates state accordingly

  * st: current state
  * node: identifier of node to connect to
  * node-state: a *cursor*, eg. a reference to some state that might be updated
    by future messages. Should probably be replaced by some async channel for better encapsulation"
  (let [ws     (WebSocket.)
        eh     (EventHandler.)
        uri    (:uri ((keyword node) st))]

    (.listen eh ws EventType.MESSAGE (partial on-msg node-state node))
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

