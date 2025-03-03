package Me.Teenaapje.Referral;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer;
import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;

import Me.Teenaapje.Referral.Commands.CommandManager;
import Me.Teenaapje.Referral.Database.Database;
import Me.Teenaapje.Referral.PlaceHolders.PlaceHolders;
import Me.Teenaapje.Referral.Utils.ConfigManager;
import Me.Teenaapje.Referral.Utils.Utils;

public class ReferralCore extends JavaPlugin{
	public static ReferralCore core;

	public ConfigManager config;
	public ReferralInvites rInvites;
	public ReferralMilestone milestone;
	public Database db;
	
	// Cache for player UUIDs and names to reduce lookups
	private ConcurrentHashMap<String, UUID> nameToUuidCache = new ConcurrentHashMap<>();
	private ConcurrentHashMap<UUID, String> uuidToNameCache = new ConcurrentHashMap<>();
	
	// Cache expiration time in milliseconds (10 minutes)
	private static final long CACHE_EXPIRATION = 10 * 60 * 1000;
	private ConcurrentHashMap<String, Long> nameCacheTimestamps = new ConcurrentHashMap<>();
	private ConcurrentHashMap<UUID, Long> uuidCacheTimestamps = new ConcurrentHashMap<>();

	public void onEnable() {
		saveDefaultConfig();
		// set the plugin
		ReferralCore.core = this;

		// get the config
		config 		= new ConfigManager();
		rInvites 	= new ReferralInvites();
		milestone 	= new ReferralMilestone();
		db 			= new Database();

		// set placeholders if papi is there
		if (ConfigManager.placeholderAPIEnabled) {
			new PlaceHolders().register();
		}

		new CommandManager();

		new ReferralEvents();
		
		// Schedule cache cleanup task (every 10 minutes)
		Bukkit.getScheduler().runTaskTimerAsynchronously(this, this::cleanupCache, 12000L, 12000L);

		Utils.Console("Initialized - Enjoy");
	}

	public void onDisable() {
		db.CloseConnection();
	}

	@SuppressWarnings("deprecation")
	public Player GetPlayer(String name) {
		if (name == null || name.isEmpty()) {
			return null;
		}
		
		Player player = this.getServer().getPlayer(name);
		if (player != null) {
			// Cache the player's UUID and name
			cachePlayerInfo(player.getUniqueId(), player.getName());
			return player;
		}

		return getServer().getOfflinePlayer(name).getPlayer();
	}

	/**
	 * Get a player's UUID by name, works for both online and offline players
	 * @param name The player's name
	 * @return The UUID of the player, or null if not found
	 */
	public UUID GetPlayerUUID(String name) {
		if (name == null || name.isEmpty()) {
			return null;
		}
		
		// Check cache first
		UUID cachedUUID = nameToUuidCache.get(name.toLowerCase());
		if (cachedUUID != null && !isCacheExpired(name, false)) {
			return cachedUUID;
		}
		
		// First check online players
		Player player = this.getServer().getPlayer(name);
		if (player != null) {
			cachePlayerInfo(player.getUniqueId(), player.getName());
			return player.getUniqueId();
		}

		// Then check offline players by exact name match
		OfflinePlayer[] offlinePlayers = Bukkit.getOfflinePlayers();
		for (OfflinePlayer offlinePlayer : offlinePlayers) {
			if (offlinePlayer.getName() != null && offlinePlayer.getName().equalsIgnoreCase(name)) {
				cachePlayerInfo(offlinePlayer.getUniqueId(), offlinePlayer.getName());
				return offlinePlayer.getUniqueId();
			}
		}

		// As a fallback, try the standard method
		OfflinePlayer offlinePlayer = Bukkit.getOfflinePlayer(name);
		if (offlinePlayer != null && offlinePlayer.hasPlayedBefore()) {
			cachePlayerInfo(offlinePlayer.getUniqueId(), offlinePlayer.getName());
			return offlinePlayer.getUniqueId();
		}

		return null;
	}

	/**
	 * Get a player's name by UUID
	 * @param uuid The player's UUID
	 * @return The name of the player, or null if not found
	 */
	public String GetPlayerName(UUID uuid) {
		if (uuid == null) {
			return null;
		}
		
		// Check cache first
		String cachedName = uuidToNameCache.get(uuid);
		if (cachedName != null && !isCacheExpired(uuid, true)) {
			return cachedName;
		}
		
		// First check online players
		Player player = this.getServer().getPlayer(uuid);
		if (player != null) {
			cachePlayerInfo(uuid, player.getName());
			return player.getName();
		}

		// Then check offline players
		OfflinePlayer offlinePlayer = Bukkit.getOfflinePlayer(uuid);
		if (offlinePlayer != null && offlinePlayer.getName() != null) {
			cachePlayerInfo(uuid, offlinePlayer.getName());
			return offlinePlayer.getName();
		}

		return null;
	}
	
	/**
	 * Cache a player's UUID and name
	 * @param uuid The player's UUID
	 * @param name The player's name
	 */
	private void cachePlayerInfo(UUID uuid, String name) {
		if (uuid == null || name == null || name.isEmpty()) {
			return;
		}
		
		nameToUuidCache.put(name.toLowerCase(), uuid);
		uuidToNameCache.put(uuid, name);
		
		// Update timestamps
		long now = System.currentTimeMillis();
		nameCacheTimestamps.put(name.toLowerCase(), now);
		uuidCacheTimestamps.put(uuid, now);
	}
	
	/**
	 * Check if a cache entry is expired
	 * @param key The cache key (either name or UUID)
	 * @param isUUID Whether the key is a UUID or a name
	 * @return True if the cache entry is expired, false otherwise
	 */
	private boolean isCacheExpired(Object key, boolean isUUID) {
		long now = System.currentTimeMillis();
		Long timestamp = isUUID ? 
				uuidCacheTimestamps.get(key) : 
				nameCacheTimestamps.get(key.toString().toLowerCase());
		
		return timestamp == null || (now - timestamp > CACHE_EXPIRATION);
	}
	
	/**
	 * Clean up expired cache entries
	 */
	private void cleanupCache() {
		long now = System.currentTimeMillis();
		
		// Clean up name cache
		nameCacheTimestamps.entrySet().removeIf(entry -> now - entry.getValue() > CACHE_EXPIRATION);
		nameToUuidCache.keySet().removeIf(key -> !nameCacheTimestamps.containsKey(key));
		
		// Clean up UUID cache
		uuidCacheTimestamps.entrySet().removeIf(entry -> now - entry.getValue() > CACHE_EXPIRATION);
		uuidToNameCache.keySet().removeIf(key -> !uuidCacheTimestamps.containsKey(key));
	}

	public void UseCommands(List<?> commands, Player player) {
		if (commands == null || player == null) {
			getLogger().warning("Attempted to use commands with null commands or player");
			return;
		}
		
		for (int i = 0; i < commands.size(); i++) {
			String command = (String) commands.get(i);
			if (command == null || command.isEmpty()) {
				continue;
			}

			final String finalCommand = command.replace("<player>", player.getName());
			getServer().getScheduler().runTask(this, new Runnable() {
				@Override
				public void run() {
					try {
						getServer().dispatchCommand(getServer().getConsoleSender(), finalCommand);
						Utils.Console("Executed command for " + player.getName() + ": " + finalCommand);
					} catch (Exception e) {
						getLogger().severe("Error executing command: " + finalCommand);
						e.printStackTrace();
					}
				}
			});
		}
	}
}