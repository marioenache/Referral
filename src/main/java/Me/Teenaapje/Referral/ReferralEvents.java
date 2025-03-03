package Me.Teenaapje.Referral;

import java.util.ArrayList;

import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;

import Me.Teenaapje.Referral.Utils.ConfigManager;
import Me.Teenaapje.Referral.Utils.Utils;
import net.md_5.bungee.api.ChatColor;
import net.md_5.bungee.api.chat.ClickEvent;
import net.md_5.bungee.api.chat.TextComponent;

public class ReferralEvents implements Listener {

	private ReferralCore core = ReferralCore.core;

	public ReferralEvents() {
		core.getServer().getPluginManager().registerEvents(this, core);
	}

	@EventHandler(priority = EventPriority.NORMAL)
	public void onPlayerJoin(PlayerJoinEvent event) {
		final Player player = event.getPlayer();
		
		// Run database operations async to avoid lag
		core.getServer().getScheduler().runTaskAsynchronously(core, () -> {
			// Create or update player in database
			core.db.CreatePlayer(player.getUniqueId().toString(), player.getName());
			
			// Check for pending offline referrals
			core.db.CheckPendingReferrals(player);
			
			// Check if there are any pending referral invites for this player
			ArrayList<Refer> pendingInvites = core.rInvites.GetPendingInvites(player.getName());
			
			if (!pendingInvites.isEmpty()) {
				// Wait a bit to send the messages after the player has fully joined
				core.getServer().getScheduler().runTaskLater(core, () -> {
					for (Refer invite : pendingInvites) {
						// Get the referring player's name
						String referrerName = invite.refer;
						
						// Create the accept/decline buttons
						TextComponent accept = Utils.CreateTextComponent(core.config.accept, ChatColor.GREEN, true, ClickEvent.Action.RUN_COMMAND, "/ref accept " + referrerName);
						TextComponent decline = Utils.CreateTextComponent(core.config.decline, ChatColor.RED, true, ClickEvent.Action.RUN_COMMAND, "/ref reject " + referrerName);
						
						// Send the invitation message
						Utils.SendMessage(player, "§aYou have a pending referral invitation from §e" + referrerName + "§a.");
						player.spigot().sendMessage(accept, decline);
					}
				}, 40L); // 2 seconds (40 ticks) delay
			}
			
			// Show referral notification for new players if enabled
			if (ConfigManager.enableNotification && player.getFirstPlayed() == player.getLastPlayed()) {
				core.getServer().getScheduler().runTaskLater(core, () -> {
					Utils.SendMessage(player, core.config.referNotification);
				}, 100L); // 5 seconds (100 ticks) delay
			}
		});
	}
	
	@EventHandler(priority = EventPriority.NORMAL)
	public void onPlayerQuit(PlayerQuitEvent event) {
		final Player player = event.getPlayer();
		
		// Run database operations async to avoid lag
		core.getServer().getScheduler().runTaskAsynchronously(core, () -> {
			// Update player name in database if needed
			core.db.CreatePlayer(player.getUniqueId().toString(), player.getName());
		});
	}
}