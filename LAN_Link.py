#!/usr/init/env python3
"""
LAN-Link - All-in-One Edition
Host a room or join one from the same app. No IP addresses or ports
are ever shown - rooms are found automatically on the network, and
the host chats under their own username just like everyone else.

Run with: python3 chat_app.py
Requires tkinter (Linux: sudo apt install python3-tk)
"""

import sys
import socket
import threading
import queue
import json
import time
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox

DISCOVERY_PORT = 54545
# Dynamic port pool tracking
PORT_POOL_START = 5000
PORT_POOL_MAX_INITIAL = 5500
_dynamic_port_ceiling = PORT_POOL_MAX_INITIAL

DISCOVERY_MAGIC = b"CHATROOM_DISCOVER_V1"
SEARCH_WINDOW_SECONDS = 1.5


def get_next_available_port():
    """Dynamically yields ports, expanding the pool ceiling if all current ones are busy."""
    global _dynamic_port_ceiling
    
    # Try searching from start up to the current ceiling, plus any newly expanded slots
    while True:
        for p in range(PORT_POOL_START, _dynamic_port_ceiling + 1):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind(("0.0.0.0", p))
                # Successfully bound! Return the socket and port
                return sock, p
            except OSError:
                sock.close()
                continue
        
        # If we exhausted the pool, expand the ceiling by 1 and try again
        _dynamic_port_ceiling += 1
        print(f"[Server] Pool exhausted! Dynamically expanding port ceiling to {_dynamic_port_ceiling}...")


def discover_rooms(timeout=SEARCH_WINDOW_SECONDS):
    """Broadcasts a discovery packet on the LAN and collects room replies."""
    print(f"[Discovery] Broadcasting search packet on port {DISCOVERY_PORT}...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.settimeout(0.3)

    try:
        sock.sendto(DISCOVERY_MAGIC, ("<broadcast>", DISCOVERY_PORT))
    except OSError as e:
        print(f"[Discovery] Broadcast error: {e}")
        sock.close()
        return []

    results = {}
    end_time = time.time() + timeout
    while time.time() < end_time:
        try:
            data, addr = sock.recvfrom(1024)
        except socket.timeout:
            continue
        except OSError:
            break
        try:
            info = json.loads(data.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            continue
        room = info.get("room")
        port = info.get("port")
        if not room or not port:
            continue
        
        print(f"[Discovery] Found room '{room}' at {addr[0]}:{port}")
        key = (addr[0], port)
        results[key] = {
            "host": addr[0],
            "port": port,
            "room": room,
            "has_password": bool(info.get("has_password", False)),
        }
    sock.close()
    return list(results.values())


class RoomServer:
    """Pure networking: owns the TCP room socket and the UDP discovery responder."""

    def __init__(self, room_name, password):
        self.room_name = room_name
        self.password = password
        self.socket = None
        self.discovery_socket = None
        self.port = None
        self.clients = {}          # socket -> username
        self.lock = threading.Lock()
        self.running = False

    def start(self):
        """Binds a free port dynamically, expanding the pool if full."""
        print(f"[Server] Attempting to start room '{self.room_name}'...")
        sock, port = get_next_available_port()
        if sock is None or port is None:
            return None

        try:
            sock.listen()
            self.socket = sock
            self.port = port
        except OSError as e:
            print(f"[Server] Failed to listen on port {port}: {e}")
            sock.close()
            return None

        print(f"[Server] Successfully bound TCP server to dynamic port {self.port}.")
        self.running = True
        threading.Thread(target=self._accept_loop, daemon=True).start()
        threading.Thread(target=self._discovery_responder, daemon=True).start()
        return self.port

    def stop(self):
        print(f"[Server] Stopping room '{self.room_name}'...")
        self.running = False
        with self.lock:
            for sock in list(self.clients.keys()):
                try:
                    sock.close()
                except OSError:
                    pass
            self.clients.clear()
        if self.socket:
            try:
                self.socket.close()
            except OSError:
                pass
            self.socket = None
        print("[Server] Stopped and port freed back to system pool.")

    def _discovery_responder(self):
        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            udp_sock.bind(("0.0.0.0", DISCOVERY_PORT))
            print(f"[Server-UDP] Discovery responder bound on port {DISCOVERY_PORT}.")
        except OSError as e:
            print(f"[Server-UDP] Failed to bind discovery port: {e}")
            return
        self.discovery_socket = udp_sock
        udp_sock.settimeout(1.0)

        while self.running:
            try:
                data, addr = udp_sock.recvfrom(1024)
            except socket.timeout:
                continue
            except OSError:
                break
            if data == DISCOVERY_MAGIC:
                print(f"[Server-UDP] Answering discovery request from {addr}")
                reply = json.dumps({
                    "room": self.room_name,
                    "port": self.port,
                    "has_password": bool(self.password),
                }).encode("utf-8")
                try:
                    udp_sock.sendto(reply, addr)
                except OSError:
                    pass
        udp_sock.close()

    def _accept_loop(self):
        while self.running:
            try:
                sock, addr = self.socket.accept()
                print(f"[Server-TCP] Incoming connection from {addr}")
            except OSError:
                break
            threading.Thread(target=self._handle_client, args=(sock, addr), daemon=True).start()

    def _broadcast(self, message, exclude=None):
        with self.lock:
            dead = []
            for sock in self.clients:
                if sock is exclude:
                    continue
                try:
                    sock.sendall(message.encode("utf-8"))
                except OSError:
                    dead.append(sock)
            for sock in dead:
                print(f"[Server-TCP] Cleaning up dead socket: {sock}")
                self.clients.pop(sock, None)

    def _handle_client(self, sock, addr):
        username = None
        was_member = False
        try:
            sock.sendall(b"ROOM_NAME:")
            submitted_room = sock.recv(1024).decode("utf-8").strip()
            if submitted_room != self.room_name:
                print(f"[Server-TCP] Connection {addr} rejected: Wrong room name '{submitted_room}'")
                sock.sendall(b"REJECT:Wrong room name\n")
                return

            sock.sendall(b"ROOM_PASSWORD:")
            submitted_pw = sock.recv(1024).decode("utf-8").strip()
            if submitted_pw != self.password:
                print(f"[Server-TCP] Connection {addr} rejected: Wrong password")
                sock.sendall(b"REJECT:Wrong password\n")
                return

            sock.sendall(b"OK:Enter your username: ")
            username = sock.recv(1024).decode("utf-8").strip()
            if not username:
                username = f"user_{addr[1]}"

            with self.lock:
                if username in self.clients.values():
                    username = f"{username}_{addr[1]}"
                self.clients[sock] = username
                was_member = True

            print(f"[Server-TCP] '{username}' successfully joined from {addr}.")
            self._broadcast(f"** {username} joined the room **\n", exclude=sock)
            sock.sendall(f"Welcome to '{self.room_name}', {username}! Type /quit to leave.\n".encode("utf-8"))

            while self.running:
                data = sock.recv(4096)
                if not data:
                    break
                text = data.decode("utf-8").strip()
                if not text:
                    continue
                if text == "/quit":
                    break
                if text == "/who":
                    with self.lock:
                        names = ", ".join(self.clients.values())
                    sock.sendall(f"Online: {names}\n".encode("utf-8"))
                    continue

                print(f"[Server-TCP] Msg from {username}: {text}")
                timestamp = time.strftime("%H:%M:%S")
                self._broadcast(f"[{timestamp}] {username}: {text}\n")

        except (ConnectionResetError, ConnectionAbortedError, OSError) as e:
            print(f"[Server-TCP] Client {username if username else addr} abruptly disconnected: {e}")
        finally:
            with self.lock:
                self.clients.pop(sock, None)
            try:
                sock.close()
            except OSError:
                pass
            if username and was_member:
                print(f"[Server-TCP] '{username}' disconnected.")
                self._broadcast(f"** {username} left the room **\n")


class ChatApp:
    def __init__(self, root):
        self.root = root
        self.root.title("LAN-Link")
        self.root.geometry("560x600")
        self.root.minsize(460, 450)

        self.sock = None
        self.connected = False
        self.is_host = False
        self.room_server = None
        self.event_queue = queue.Queue()
        
        # Pagination & Filter Variables
        self.all_discovered_rooms = []
        self.filtered_rooms = []
        self.displayed_rooms = []
        self.rooms_loaded_count = 0
        self.selected_room = None

        self._build_debug_window()

        self.debug_btn = ttk.Button(self.root, text="🐞 Toggle Debug Console", command=self._toggle_debug)
        self.debug_btn.pack(side="bottom", fill="x", pady=2)

        self._build_home_frame()
        self._build_host_frame()
        self._build_search_frame()
        self._build_join_frame()
        self._build_chat_frame()
        self._show_home()

        self.root.after(100, self._drain_queue)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        print("[App] LAN-Link started.")

    # ---------- UI: Debug Console ----------
    def _build_debug_window(self):
        self.debug_window = tk.Toplevel(self.root)
        self.debug_window.title("Debug Console")
        self.debug_window.geometry("550x400")
        self.debug_window.withdraw()
        self.debug_window.protocol("WM_DELETE_WINDOW", self.debug_window.withdraw)

        self.debug_text = scrolledtext.ScrolledText(self.debug_window, state="disabled", wrap="word", font=("Consolas", 9))
        self.debug_text.pack(fill="both", expand=True, padx=5, pady=5)
        
        class StdoutRedirector:
            def __init__(self, eq):
                self.eq = eq
            def write(self, msg):
                if msg:
                    self.eq.put(("debug", msg))
            def flush(self): pass
            
        sys.stdout = StdoutRedirector(self.event_queue)
        sys.stderr = sys.stdout

    def _toggle_debug(self):
        if self.debug_window.state() == "withdrawn":
            self.debug_window.deiconify()
        else:
            self.debug_window.withdraw()

    # ---------- UI: home screen ----------
    def _build_home_frame(self):
        self.home_frame = ttk.Frame(self.root, padding=30)
        
        # Python branding header (LAN in yellow, Link in blue)
        title_frame = ttk.Frame(self.home_frame)
        title_frame.pack(pady=(20, 2))
        
        lbl_lan = tk.Label(title_frame, text="LAN", font=("", 20, "bold"), fg="#FFD43B", bg=self.root.cget("bg"))
        lbl_lan.pack(side="left")
        lbl_dash = tk.Label(title_frame, text="-", font=("", 20, "bold"), fg="gray", bg=self.root.cget("bg"))
        lbl_dash.pack(side="left")
        lbl_link = tk.Label(title_frame, text="Link", font=("", 20, "bold"), fg="#306998", bg=self.root.cget("bg"))
        lbl_link.pack(side="left")

        # Subtitle: Made by Daniel Cave
        ttk.Label(self.home_frame, text="made by Daniel Cave", font=("", 9, "italic"), foreground="gray").pack(pady=(0, 10))

        ttk.Label(self.home_frame, text="Chat with anyone on your local network.",
                  foreground="gray").pack(pady=(0, 30))
        ttk.Button(self.home_frame, text="Host a Room", command=self._show_host).pack(
            fill="x", ipady=8, pady=6)
        ttk.Button(self.home_frame, text="Join a Room", command=self._enter_search).pack(
            fill="x", ipady=8, pady=6)

    # ---------- UI: host setup screen ----------
    def _build_host_frame(self):
        self.host_frame = ttk.Frame(self.root, padding=20)
        ttk.Label(self.host_frame, text="Host a Room", font=("", 12, "bold")).pack(anchor="w", pady=(0, 12))

        fields = ttk.Frame(self.host_frame)
        fields.pack(fill="x")

        ttk.Label(fields, text="Room name:").grid(row=0, column=0, sticky="w", pady=4)
        self.host_room_entry = ttk.Entry(fields)
        self.host_room_entry.insert(0, "My Room")
        self.host_room_entry.grid(row=0, column=1, sticky="ew", pady=4)

        ttk.Label(fields, text="Password (optional):").grid(row=1, column=0, sticky="w", pady=4)
        self.host_pw_entry = ttk.Entry(fields, show="*")
        self.host_pw_entry.grid(row=1, column=1, sticky="ew", pady=4)

        ttk.Label(fields, text="Your username:").grid(row=2, column=0, sticky="w", pady=4)
        self.host_username_entry = ttk.Entry(fields)
        self.host_username_entry.grid(row=2, column=1, sticky="ew", pady=4)
        self.host_username_entry.bind("<Return>", lambda e: self.start_hosting())

        fields.columnconfigure(1, weight=1)

        self.host_status_var = tk.StringVar(value="")
        ttk.Label(self.host_frame, textvariable=self.host_status_var, foreground="red").pack(pady=(10, 0))

        btns = ttk.Frame(self.host_frame)
        btns.pack(pady=10)
        ttk.Button(btns, text="Back", command=self._show_home).pack(side="left")
        self.start_hosting_btn = ttk.Button(btns, text="Start Hosting", command=self.start_hosting)
        self.start_hosting_btn.pack(side="left", padx=(6, 0))

    # ---------- UI: room search screen ----------
    def _build_search_frame(self):
        self.search_frame = ttk.Frame(self.root, padding=16)

        header = ttk.Frame(self.search_frame)
        header.pack(fill="x")
        ttk.Label(header, text="Rooms on your network", font=("", 12, "bold")).pack(side="left")
        self.search_btn = ttk.Button(header, text="Refresh Search", command=self.search_rooms)
        self.search_btn.pack(side="right")

        self.search_status_var = tk.StringVar(value="")
        ttk.Label(self.search_frame, textvariable=self.search_status_var, foreground="gray").pack(
            anchor="w", pady=(4, 4))

        # Filtering Frame
        filter_frame = ttk.Frame(self.search_frame)
        filter_frame.pack(fill="x", pady=(0, 8))
        ttk.Label(filter_frame, text="Filter by name:").pack(side="left")
        self.room_filter_var = tk.StringVar()
        self.room_filter_var.trace_add("write", lambda *args: self._apply_filter())
        self.room_filter_entry = ttk.Entry(filter_frame, textvariable=self.room_filter_var)
        self.room_filter_entry.pack(side="left", fill="x", expand=True, padx=(6, 0))

        # List Frame (holds listbox and load more button)
        list_container = ttk.Frame(self.search_frame)
        list_container.pack(fill="both", expand=True)
        
        self.load_more_btn = ttk.Button(list_container, text="Load 25 More", command=self._load_more_rooms)
        
        list_box_frame = ttk.Frame(list_container)
        list_box_frame.pack(fill="both", expand=True, side="top", pady=(0, 4))
        self.rooms_listbox = tk.Listbox(list_box_frame, activestyle="dotbox")
        self.rooms_listbox.pack(side="left", fill="both", expand=True)
        scrollbar = ttk.Scrollbar(list_box_frame, command=self.rooms_listbox.yview)
        scrollbar.pack(side="right", fill="y")
        self.rooms_listbox.configure(yscrollcommand=scrollbar.set)
        self.rooms_listbox.bind("<Double-Button-1>", lambda e: self.select_room())

        btns = ttk.Frame(self.search_frame)
        btns.pack(fill="x", pady=(10, 0))
        ttk.Button(btns, text="Back", command=self._show_home).pack(side="left")
        ttk.Button(btns, text="Join Selected Room", command=self.select_room).pack(side="right")

    # ---------- UI: join (password/username) screen ----------
    def _build_join_frame(self):
        self.join_frame = ttk.Frame(self.root, padding=20)

        self.join_room_label_var = tk.StringVar(value="")
        ttk.Label(self.join_frame, textvariable=self.join_room_label_var,
                  font=("", 12, "bold")).pack(anchor="w", pady=(0, 12))

        fields = ttk.Frame(self.join_frame)
        fields.pack(fill="x")

        self.pw_label = ttk.Label(fields, text="Room password:")
        self.pw_label.grid(row=0, column=0, sticky="w", pady=4)
        self.pw_entry = ttk.Entry(fields, show="*")
        self.pw_entry.grid(row=0, column=1, sticky="ew", pady=4)

        ttk.Label(fields, text="Your username:").grid(row=1, column=0, sticky="w", pady=4)
        self.username_entry = ttk.Entry(fields)
        self.username_entry.grid(row=1, column=1, sticky="ew", pady=4)
        self.username_entry.bind("<Return>", lambda e: self.confirm_join())

        fields.columnconfigure(1, weight=1)

        self.join_status_var = tk.StringVar(value="")
        ttk.Label(self.join_frame, textvariable=self.join_status_var, foreground="red").pack(pady=(10, 0))

        btns = ttk.Frame(self.join_frame)
        btns.pack(pady=10)
        ttk.Button(btns, text="Back", command=self._show_search).pack(side="left")
        self.confirm_join_btn = ttk.Button(btns, text="Join", command=self.confirm_join)
        self.confirm_join_btn.pack(side="left", padx=(6, 0))

    # ---------- UI: chat screen ----------
    def _build_chat_frame(self):
        self.chat_frame = ttk.Frame(self.root)

        header = ttk.Frame(self.chat_frame, padding=(10, 6))
        header.pack(fill="x")
        self.header_var = tk.StringVar(value="")
        ttk.Label(header, textvariable=self.header_var, font=("", 10, "bold")).pack(side="left")
        ttk.Button(header, text="Who's online", command=self.request_who).pack(side="right")
        self.leave_btn = ttk.Button(header, text="Leave", command=self.leave_room)
        self.leave_btn.pack(side="right", padx=(0, 6))

        body = ttk.Frame(self.chat_frame, padding=(10, 0, 10, 10))
        body.pack(fill="both", expand=True)
        self.chat_box = scrolledtext.ScrolledText(body, state="disabled", wrap="word")
        self.chat_box.pack(fill="both", expand=True)

        entry_frame = ttk.Frame(self.chat_frame, padding=(10, 0, 10, 10))
        entry_frame.pack(fill="x")
        self.msg_entry = ttk.Entry(entry_frame)
        self.msg_entry.pack(side="left", fill="x", expand=True)
        self.msg_entry.bind("<Return>", lambda event: self.send_message())
        ttk.Button(entry_frame, text="Send", command=self.send_message).pack(side="left", padx=(6, 0))

    # ---------- Screen switching ----------
    def _hide_all(self):
        for frame in (self.home_frame, self.host_frame, self.search_frame,
                      self.join_frame, self.chat_frame):
            frame.pack_forget()

    def _show_home(self):
        self._hide_all()
        self.host_status_var.set("")
        self.home_frame.pack(fill="both", expand=True)

    def _show_host(self):
        self._hide_all()
        self.host_status_var.set("")
        self.host_frame.pack(fill="both", expand=True)

    def _enter_search(self):
        self._hide_all()
        self.search_frame.pack(fill="both", expand=True)
        self.search_rooms()

    def _show_search(self):
        self._hide_all()
        self.search_frame.pack(fill="both", expand=True)

    def _show_join(self):
        self._hide_all()
        self.join_frame.pack(fill="both", expand=True)

    def _show_chat(self):
        self._hide_all()
        self.leave_btn.configure(text="Close Room" if self.is_host else "Leave")
        self.chat_frame.pack(fill="both", expand=True)
        self.msg_entry.focus_set()

    def _append_chat(self, text):
        self.chat_box.configure(state="normal")
        self.chat_box.insert("end", text if text.endswith("\n") else text + "\n")
        self.chat_box.see("end")
        self.chat_box.configure(state="disabled")

    # ---------- Queue draining (GUI thread) ----------
    def _drain_queue(self):
        try:
            while True:
                kind, payload = self.event_queue.get_nowait()
                if kind == "debug":
                    self.debug_text.configure(state="normal")
                    self.debug_text.insert("end", payload)
                    self.debug_text.see("end")
                    self.debug_text.configure(state="disabled")
                elif kind == "rooms_found":
                    self._populate_rooms(payload)
                elif kind == "chat":
                    self._append_chat(payload)
                elif kind == "join_error":
                    self.join_status_var.set(payload)
                    self.confirm_join_btn.configure(state="normal")
                elif kind == "host_error":
                    self.host_status_var.set(payload)
                    self.start_hosting_btn.configure(state="normal")
                elif kind == "joined":
                    room, username = payload
                    label = f"Hosting: {room}" if self.is_host else f"Room: {room}"
                    self.header_var.set(f"{label}  |  You are: {username}")
                    self._show_chat()
                elif kind == "disconnected":
                    if self.connected:
                        self.connected = False
                        messagebox.showinfo("Disconnected", payload)
                        self._reset_to_home()
        except queue.Empty:
            pass
        self.root.after(100, self._drain_queue)

    # ---------- Room discovery, Filtering & Pagination ----------
    def search_rooms(self):
        self.search_status_var.set("Searching for rooms...")
        self.search_btn.configure(state="disabled")
        self.rooms_listbox.delete(0, "end")
        self.load_more_btn.pack_forget()
        threading.Thread(target=self._search_rooms_bg, daemon=True).start()

    def _search_rooms_bg(self):
        rooms = discover_rooms()
        self.event_queue.put(("rooms_found", rooms))

    def _populate_rooms(self, rooms):
        self.all_discovered_rooms = rooms
        self._apply_filter()
        self.search_btn.configure(state="normal")

    def _apply_filter(self):
        query = self.room_filter_var.get().strip().lower()
        if query:
            self.filtered_rooms = [r for r in self.all_discovered_rooms if query in r["room"].lower()]
        else:
            self.filtered_rooms = self.all_discovered_rooms.copy()
            
        self.rooms_loaded_count = 0
        self.displayed_rooms = []
        self.rooms_listbox.delete(0, "end")
        self.load_more_btn.pack_forget()

        if not self.all_discovered_rooms:
            self.search_status_var.set("No rooms found. Make sure a host has started one.")
        else:
            self.search_status_var.set(f"Found {len(self.all_discovered_rooms)} room(s). Matches: {len(self.filtered_rooms)}.")
            self._load_more_rooms()

    def _load_more_rooms(self):
        start = self.rooms_loaded_count
        end = start + 25
        chunk = self.filtered_rooms[start:end]
        
        for room in chunk:
            self.displayed_rooms.append(room)
            label = room["room"] + ("  🔒" if room["has_password"] else "")
            self.rooms_listbox.insert("end", label)
            
        self.rooms_loaded_count += len(chunk)
        
        if self.rooms_loaded_count < len(self.filtered_rooms):
            self.load_more_btn.pack(side="bottom", fill="x")
        else:
            self.load_more_btn.pack_forget()

    def select_room(self):
        selection = self.rooms_listbox.curselection()
        if not selection:
            messagebox.showinfo("Pick a room", "Select a room from the list first.")
            return
        
        self.selected_room = self.displayed_rooms[selection[0]]
        self.join_room_label_var.set(f"Joining: {self.selected_room['room']}")
        self.join_status_var.set("")
        self.pw_entry.delete(0, "end")
        self.username_entry.delete(0, "end")

        if self.selected_room["has_password"]:
            self.pw_label.grid()
            self.pw_entry.grid()
        else:
            self.pw_label.grid_remove()
            self.pw_entry.grid_remove()

        self._show_join()

    # ---------- Hosting ----------
    def start_hosting(self):
        room = self.host_room_entry.get().strip()
        if not room:
            self.host_status_var.set("Room name is required.")
            return
        username = self.host_username_entry.get().strip()
        if not username:
            self.host_status_var.set("Username is required.")
            return
        password = self.host_pw_entry.get()

        self.host_status_var.set("Starting room...")
        self.start_hosting_btn.configure(state="disabled")
        threading.Thread(target=self._start_hosting_bg, args=(room, password, username), daemon=True).start()

    def _start_hosting_bg(self, room, password, username):
        server = RoomServer(room, password)
        port = server.start()
        if port is None:
            self.event_queue.put(("host_error", "Failed to bind network port for room."))
            return

        self.room_server = server
        self.is_host = True
        self._connect_and_login("127.0.0.1", port, room, password, username, is_host_error_target="host_error")

    # ---------- Connection logic ----------
    def confirm_join(self):
        if not self.selected_room:
            return
        username = self.username_entry.get().strip()
        if not username:
            self.join_status_var.set("Username is required.")
            return
        password = self.pw_entry.get() if self.selected_room["has_password"] else ""

        self.join_status_var.set("Connecting...")
        self.confirm_join_btn.configure(state="disabled")
        self.is_host = False
        threading.Thread(
            target=self._connect_and_login,
            args=(self.selected_room["host"], self.selected_room["port"],
                  self.selected_room["room"], password, username),
            kwargs={"is_host_error_target": "join_error"},
            daemon=True,
        ).start()

    def _connect_and_login(self, host, port, room, password, username, is_host_error_target="join_error"):
        print(f"[Client] Attempting handshake with {host}:{port}...")
        try:
            sock = socket.create_connection((host, port), timeout=8)

            sock.recv(1024)
            sock.sendall(room.encode("utf-8"))

            reply = sock.recv(1024).decode("utf-8")
            if reply.startswith("REJECT:"):
                print(f"[Client] Server rejected connection: {reply}")
                sock.close()
                self.event_queue.put((is_host_error_target, reply.split(":", 1)[1].strip()))
                return
            
            sock.sendall(password.encode("utf-8"))

            reply = sock.recv(1024).decode("utf-8")
            if reply.startswith("REJECT:"):
                print(f"[Client] Server rejected connection (Bad Password): {reply}")
                sock.close()
                self.event_queue.put((is_host_error_target, reply.split(":", 1)[1].strip()))
                return
            
            sock.sendall(username.encode("utf-8"))
            welcome = sock.recv(1024).decode("utf-8")
            print("[Client] Handshake successful.")
            
            sock.settimeout(None)

        except (OSError, socket.timeout) as e:
            print(f"[Client] Connection failed: {e}")
            self.event_queue.put((is_host_error_target, f"Could not connect: {e}"))
            return

        self.sock = sock
        self.connected = True
        self.event_queue.put(("chat", welcome.strip()))
        self.event_queue.put(("joined", (room, username)))
        threading.Thread(target=self._receive_loop, daemon=True).start()

    def _receive_loop(self):
        while self.connected:
            try:
                data = self.sock.recv(4096)
            except OSError:
                break
            if not data:
                print("[Client] Server closed the connection.")
                break
            self.event_queue.put(("chat", data.decode("utf-8")))
        self.event_queue.put(("disconnected", "Lost connection to the room."))

    def send_message(self):
        if not self.connected or not self.sock:
            return
        msg = self.msg_entry.get().strip()
        if not msg:
            return
        try:
            self.sock.sendall(msg.encode("utf-8"))
        except OSError as e:
            print(f"[Client] Send error: {e}")
            self.event_queue.put(("disconnected", "Lost connection to the room."))
            return
        self.msg_entry.delete(0, "end")

    def request_who(self):
        if self.connected and self.sock:
            try:
                self.sock.sendall(b"/who")
            except OSError:
                pass

    def leave_room(self):
        print("[Client] Leaving room...")
        if self.connected and self.sock:
            try:
                self.sock.sendall(b"/quit")
            except OSError:
                pass
        self._close_socket()
        self.connected = False
        if self.is_host and self.room_server:
            self.room_server.stop()
            self.room_server = None
        self.is_host = False
        self._reset_to_home()

    def _close_socket(self):
        if self.sock:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None

    def _reset_to_home(self):
        self._close_socket()
        self.chat_box.configure(state="normal")
        self.chat_box.delete("1.0", "end")
        self.chat_box.configure(state="disabled")
        self.join_status_var.set("")
        self.host_status_var.set("")
        self.confirm_join_btn.configure(state="normal")
        self.start_hosting_btn.configure(state="normal")
        self.selected_room = None
        self._show_home()

    def _on_close(self):
        print("[App] Shutting down...")
        if self.connected and self.sock:
            try:
                self.sock.sendall(b"/quit")
            except OSError:
                pass
        self._close_socket()
        if self.is_host and self.room_server:
            self.room_server.stop()
        self.root.destroy()


def main():
    root = tk.Tk()
    app = ChatApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
