package com.aerospike.timf.client;

import java.io.IOException;
import java.util.EnumSet;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.eclipse.jetty.webapp.WebAppContext;

import com.aerospike.timf.client.ui.IWebRequestProcessor;

import jakarta.servlet.DispatcherType;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class PortListener {
    private Server server;
    private static final int MAX_THREADS = 100; 
    private static int MIN_THREADS = 3;
    private static int IDLE_TIMEOUT = 120;
    
    private static final String CONTEXT_PATH = "/";
    private static final String RESOURCE_PATH = "/static/aerospike-perf-monitor-jquery";
    
    public void start(int port, IWebRequestProcessor processor) throws Exception {
    	server = new Server();
    	ServerConnector connector = new ServerConnector(server);
          connector.setPort(port);
    	  server.setConnectors(new Connector[]{connector});
    	  ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    	  context.setContextPath("/api");
    	  ServletHolder servletHolder = new ServletHolder(new ProcessingServlet(processor));
    	  servletHolder.setInitOrder(1);
    	  context.addServlet(servletHolder, "/*");
    	  
    	  //Allow cross origin from local hosts
    		FilterHolder filter = new FilterHolder();
    		filter.setInitParameter("allowedOrigins", "http://localhost:3000,http://localhost:3003");
    		filter.setInitParameter("allowedMethods", "POST,GET,OPTIONS,PUT,DELETE,HEAD");
    		filter.setInitParameter("allowedHeaders", "X-PINGOTHER, Origin, X-Requested-With, Content-Type, Accept");
    		filter.setInitParameter("preflightMaxAge", "728000");
    		filter.setInitParameter("allowCredentials", "true");
    		CrossOriginFilter corsFilter = new CrossOriginFilter();
    		filter.setFilter(corsFilter);
			context.addFilter(filter, "/*", EnumSet.of(DispatcherType.REQUEST));


    	  WebAppContext webAppContext = new WebAppContext();
    	  webAppContext.setServer(server);
    	  webAppContext.setContextPath(CONTEXT_PATH);
    	  webAppContext.setResourceBase(PortListener.class.getResource(RESOURCE_PATH).toExternalForm());
    	  // Serve files out of any folder
    	  webAppContext.setInitParameter("pathInfoOnly", "true");
    	  webAppContext.setInitParameter("cacheControl", "max-age=0, public");

    	  HandlerList handlers = new HandlerList();
    	  handlers.setHandlers(new Handler[]{context, webAppContext});
    	  server.setHandler(handlers);
    	  server.setStopAtShutdown(true);
    	  server.start();
    }
    
    /*
    public void start2(int port) throws Exception {
    	QueuedThreadPool threadPool = new QueuedThreadPool(MAX_THREADS, MIN_THREADS, IDLE_TIMEOUT);
        server = new Server(threadPool);

    	// ---JSON RESOURCE---//

//        JSONResource resource = new JSONResource();
//
//        ResourceConfig rc = new ResourceConfig();
//        rc.register(resource);
//
//        ServletContainer sc = new ServletContainer(rc);
//
//        ServletHolder servletHolder = new ServletHolder(sc);

//        ServletHandler servletHandler = new ServletHandler();
//        servletHandler.addServletWithMapping(BlockingServlet.class, "/status");

//        ServletContextHandler jsonResourceContext = new ServletContextHandler();
//        jsonResourceContext.addServlet(servletHolder, "/*");
        ServletContextHandler contextHandler = new ServletContextHandler();
        contextHandler.addServlet(ProcessingServlet.class, "/status");

        // ---STATIC RESOURCE---//
        ResourceHandler staticResourceHandler = new ResourceHandler();
        staticResourceHandler.setResourceBase("./src/main/resources/static");
        ContextHandler staticContextHandler = new ContextHandler("/");
        staticContextHandler.setHandler(staticResourceHandler);

        // ---ADD HANDLERS---//
        HandlerList handlers = new HandlerList();
        handlers.setHandlers(new Handler[] { contextHandler, //
                staticContextHandler, //
                new DefaultHandler() //
        });

        server.setHandler(handlers);
        
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(port);
        server.setConnectors(new Connector[] {connector});
        
        server.start();
    }
    
    
    public void start1(int port) throws Exception {
    	QueuedThreadPool threadPool = new QueuedThreadPool(MAX_THREADS, MIN_THREADS, IDLE_TIMEOUT);
    	threadPool.setName("Jetty Server");
    	
        server = new Server(threadPool);
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(port);
        server.setConnectors(new Connector[] {connector});
        
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");

        ServletHolder staticHolder = new ServletHolder(new DefaultServlet());
        staticHolder.setInitParameter("resourceBase", "./src/main/resources/static");
        staticHolder.setInitParameter("pathInfoOnly", "true");
        context.addServlet(staticHolder, "/app/*");

        ServletContextHandler contextHandler = new ServletContextHandler();
        contextHandler.addServlet(ProcessingServlet.class, "/status");

        // ---ADD HANDLERS---//
        HandlerList handlers = new HandlerList();
        handlers.setHandlers(new Handler[] { 
        		contextHandler,
        		context, 
                new DefaultHandler() //
        });

        server.setHandler(handlers);

        
//        server.setHandler(this.configure());
//        server.setHandler(context);
        server.start();
//        server.join();
    }
    */
    public static class ProcessingServlet extends HttpServlet {
    	private IWebRequestProcessor processor;
    	
    	public ProcessingServlet(IWebRequestProcessor processor) {
    		this.processor = processor;
		}
    	
        protected void doGet(
          HttpServletRequest request, 
          HttpServletResponse response)
          throws ServletException, IOException {
     
    		String dataResponse = this.processor.process(request);
    		response.getWriter().println(dataResponse);
            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
        }
    }
    
    public void close() {
        try {
            this.server.stop();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        this.server.destroy();
    }

}
