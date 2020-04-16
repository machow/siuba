window.addEventListener('load', () => {
  // Cache selectors
  var topMenu = $(".sphinxsidebarwrapper"),
      topMenuHeight = 50, //topMenu.outerHeight()+15,
      // All list items
      menuItems = topMenu.find('a[href^="#"]'),
      // Anchors corresponding to menu items
      scrollItems = menuItems.map(function(){
        var hash = $(this).attr("href")
        if (hash == "#") { return }

        var item = $('a.headerlink[href^="' + hash + '"]')
        if (item.length) { return item; }
      });

  var crntHash = null;
  
  // Bind to scroll
  $(window).scroll(function(){
     // Get container scroll position
     var fromTop = $(this).scrollTop()+topMenuHeight;
  
     // Get id of current scroll item
     var cur = scrollItems.map(function(){
       if ($(this).offset().top < fromTop)
         return this;
     });
     // Get the id of the current element
     cur = cur[cur.length-1];
     var hash = cur && cur.length ? cur[0].hash : "";

     if (hash != crntHash) {
       crntHash = hash
       // Set/remove active class
       menuItems
         //.parent()
         .removeClass("active")
         //.end()
         .filter('[href="'+hash+'"]')
         //.parent()
         .addClass("active");
     }
  });
})


